package com.nec.arrow.colvector

import com.nec.arrow.colvector.ArrowVectorConversions._
import com.nec.spark.agile.core._
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.{BytePointer, IntPointer}

/**
 * Storage of a col vector as serialized Arrow buffers, that are in BytePointers.
 * We use Option[] because the `container` has no BytePointer.
 */

final case class BytePointerColVector(underlying: GenericColVector[Option[BytePointer]]) {

  def toVeColVector()(implicit
    veProcess: VeProcess,
    _source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColVector =
    VeColVector(
      transferBuffersToVe()
        .map(_.getOrElse(-1))
    ).newContainer()

  def transferBuffersToVe()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): GenericColVector[Option[Long]] = {
    cycloneMetrics.measureRunningTime(
      underlying
        .map(_.map(bp => {
          veProcess.putPointer(bp)
        }))
        .copy(source = source)
    )(cycloneMetrics.registerTransferTime)
  }

  def toByteArrayColVector(): ByteArrayColVector = {
    ByteArrayColVector(
      underlying.copy(
        container = None,
        buffers = underlying
          .map(_.map(bp => {
            try bp.asBuffer.array()
            catch {
              case _: UnsupportedOperationException =>
                val size = bp.limit()
                val target: Array[Byte] = Array.fill(size.toInt)(-1)
                bp.get(target)
                target
            }
          }))
          .buffers
      )
    )
  }
}

object BytePointerColVector {


  def fromOffHeapColumnarVector(size: Int, str: String, col: OffHeapColumnVector)(implicit
    source: VeColVectorSource
  ): Option[BytePointerColVector] = {
    PartialFunction.condOpt(col.dataType()) { case IntegerType =>
      /** TODO do this in one shot - whether on the VE or via Intel CPU extensions */
      val ptr = new IntPointer(size.toLong)
      (0 until size).foreach(idx => ptr.put(idx.toLong, col.getInt(idx)))
      val validity = new IntPointer(size.toLong)

      /** TODO do this correctly - it works for some limited cases, however */
      (0 until size).foreach(idx => validity.put(idx.toLong, Int.MaxValue))

      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = size,
          name = str,
          variableSize = None,
          veType = VeNullableInt,
          container = None,
          buffers = List(Some(new BytePointer(ptr)), Some(new BytePointer(validity)))
        )
      )
    }
  }

  def fromColumnarVectorViaArrow(name: String, columnVector: ColumnVector, size: Int)(implicit
    source: VeColVectorSource,
    bufferAllocator: BufferAllocator
  ): Option[(FieldVector, BytePointerColVector)] = {
    PartialFunction.condOpt(
      columnVector.dataType() -> TypeLink.SparkToArrow.get(columnVector.dataType())
    ) {
      case (_, Some(tl)) =>
        val theVec = tl.makeArrow(name)
        theVec.setValueCount(size)
        (0 until size).foreach { idx => tl.transfer(idx, columnVector, theVec) }
        (theVec, theVec.asInstanceOf[ValueVector].toBytePointerColVector)
      case (StringType, _) =>
        val varCharVector = new VarCharVector(name, bufferAllocator)
        varCharVector.allocateNew()
        (0 until size).foreach {
          case idx if columnVector.isNullAt(idx) => varCharVector.setNull(idx)
          case idx =>
            val utf8 = columnVector.getUTF8String(idx)
            val byteBuffer = utf8.getByteBuffer
            varCharVector.setSafe(idx, byteBuffer, byteBuffer.position(), utf8.numBytes())
        }
        varCharVector.setValueCount(size)
        (varCharVector, varCharVector.toBytePointerColVector)
    }
  }
}
