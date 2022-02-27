package com.nec.arrow.colvector

import com.nec.arrow.ArrowInterfaces
import com.nec.arrow.colvector.TypeLink.{ArrowToVe, VeToArrow}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{VeNullableInt, VeNullableTimestamp}
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.colvector.VeColVector.getUnsafe
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._

import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.util.ArrowUtilsExposed.{RichSmallIntVector, RichTimestampMicroVector}
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.{BytePointer, IntPointer}
import java.nio.ByteBuffer

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
        .map(_.map(bp => veProcess.putPointer(bp)))
        .copy(source = source)
    )(cycloneMetrics.registerTransferTime)
  }

  def toByteArrayColVector(): ByteArrayColVector =
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

  def extractBuffers()(implicit veProcess: VeProcess): List[Array[Byte]] = {
    import underlying._
    buffers.flatten
      .zip(bufferSizes)
      .map { case (targetBuf, veBufferSize) =>
        val dst = Array.fill[Byte](veBufferSize)(-1)
        targetBuf.get(dst, 0, veBufferSize)
        dst
      }
  }

  def toArrowVector()(implicit bufferAllocator: BufferAllocator): FieldVector = {
    import underlying.{buffers, numItems}
    val bytePointersAddresses = buffers.flatten.map(_.address())
    underlying.veType match {
      case VeScalarType.VeNullableTimestamp =>
        val bigIntVector = new BigIntVector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 8
          val bPointer = new BytePointer(ByteBuffer.allocateDirect(dataSize))

          bigIntVector.setValueCount(numItems)

          getUnsafe.copyMemory(
            bytePointersAddresses(0),
            bPointer.address(),
            dataSize
          )

          val longBuffer = bPointer.asBuffer().asLongBuffer()
          (0 until numItems).map(idx => bigIntVector.set(idx, longBuffer.get(idx) / 1000))
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            bigIntVector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
        }
        bigIntVector
      case VeScalarType.VeNullableShort =>
        /** Short type is casted to int */
        val smallIntVector = new SmallIntVector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 4
          smallIntVector.setValueCount(numItems)
          val buff = new BytePointer(ByteBuffer.allocateDirect(dataSize))

          getUnsafe.copyMemory(bytePointersAddresses(0), buff.address(), dataSize)
          val intBuff = buff.asBuffer().asIntBuffer()
          (0 until numItems).foreach(idx => smallIntVector.set(idx, intBuff.get(idx)))
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            smallIntVector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
        }
        smallIntVector

      case scalarType: VeScalarType if VeToArrow.contains(scalarType) =>
        val typeLink = VeToArrow(scalarType)
        val vec = typeLink.makeArrow(underlying.name)(bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * scalarType.cSize
          vec.setValueCount(numItems)
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            vec.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(bytePointersAddresses(0), vec.getDataBufferAddress, dataSize)
        }
        vec
      case VeString =>
        val vcvr = new VarCharVector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val buffersSize = numItems * 4
          val lastOffsetIndex = (numItems - 1) * 4
          val lengthTarget = new BytePointer(buffersSize)
          val startsTarget = new BytePointer(buffersSize)
          val validityTarget = new BytePointer(numItems)
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            startsTarget.address(),
            startsTarget.capacity()
          )
          getUnsafe.copyMemory(
            bytePointersAddresses(2),
            lengthTarget.address(),
            lengthTarget.capacity()
          )

          val dataSize =
            (startsTarget.getInt(lastOffsetIndex) + lengthTarget.getInt(lastOffsetIndex))
          val vhTarget = new BytePointer(dataSize * 4)

          getUnsafe.copyMemory(bytePointersAddresses(0), vhTarget.address(), vhTarget.limit())
          vcvr.allocateNew(dataSize, numItems)
          vcvr.setValueCount(numItems)
          val array = new Array[Byte](dataSize * 4)
          vhTarget.get(array)

          for (i <- 0 until numItems) {
            val start = startsTarget.getInt(i * 4) * 4
            val length = lengthTarget.getInt(i * 4) * 4
            val str = new String(array, start, length, "UTF-32LE")
            val utf8bytes = str.getBytes
            vcvr.set(i, utf8bytes)
          }
          getUnsafe.copyMemory(
            bytePointersAddresses(3),
            vcvr.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
        }
        vcvr
      case other => sys.error(s"Not supported for conversion to arrow vector: $other")
    }
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
          veType = VeScalarType.veNullableInt,
          container = None,
          buffers = List(Some(new BytePointer(ptr)), Some(new BytePointer(validity)))
        )
      )
    }
  }

  def fromArrowVector(
    valueVector: ValueVector
  )(implicit source: VeColVectorSource): BytePointerColVector =
    valueVector match {
      case timestampVector: TimeStampMicroTZVector => fromBaseFixedWidthVector(VeNullableTimestamp, timestampVector.toNanoVector)
      case smallIntVector: SmallIntVector =>
        fromBaseFixedWidthVector(VeNullableInt, smallIntVector.toIntVector)
      case vec: BaseFixedWidthVector if ArrowToVe.contains(vec.getClass) =>
        fromBaseFixedWidthVector(ArrowToVe(vec.getClass).veScalarType, vec)
      case varCharVector: VarCharVector => fromVarcharVector(varCharVector)
      case other                        => sys.error(s"Not supported to convert from ${other.getClass}")
    }

  def fromBaseFixedWidthVector(veType: VeScalarType, bigIntVector: BaseFixedWidthVector)(implicit
    source: VeColVectorSource
  ): BytePointerColVector =
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = bigIntVector.getValueCount,
        name = bigIntVector.getName,
        veType = veType,
        container = None,
        buffers = List(
          Option(new BytePointer(bigIntVector.getDataBuffer.nioBuffer())),
          Option(new BytePointer(bigIntVector.getValidityBuffer.nioBuffer()))
        ),
        variableSize = None
      )
    )

  def fromVarcharVector(
    varcharVector: VarCharVector
  )(implicit source: VeColVectorSource): BytePointerColVector = {
    val data = ArrowInterfaces.intCharsFromVarcharVector(varcharVector)
    val starts = ArrowInterfaces.startsFromVarcharVector(varcharVector)
    val lengths = ArrowInterfaces.lengthsFromVarcharVector(varcharVector)
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = varcharVector.getValueCount,
        name = varcharVector.getName,
        veType = VeString,
        container = None,
        buffers = List(
          Option(new BytePointer(data)),
          Option(new BytePointer(starts)),
          Option(new BytePointer(lengths)),
          Option(new BytePointer(varcharVector.getValidityBuffer.nioBuffer()))
        ),
        variableSize = Some(data.limit() / 4)
      )
    )
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
        (theVec, fromBaseFixedWidthVector(tl.veScalarType, theVec))
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
        (varCharVector, fromVarcharVector(varCharVector))
    }
  }

}
