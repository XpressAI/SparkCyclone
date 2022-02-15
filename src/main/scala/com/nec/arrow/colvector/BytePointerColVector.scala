package com.nec.arrow.colvector

import com.nec.arrow.ArrowInterfaces
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString}
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.colvector.VeColVector.getUnsafe
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtilsExposed.RichSmallIntVector
import org.bytedeco.javacpp.BytePointer
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerTransferTime}
import sun.nio.ch.DirectBuffer

/**
 * Storage of a col vector as serialized Arrow buffers, that are in BytePointers.
 * We use Option[] because the `container` has no BytePointer.
 */

final case class BytePointerColVector(underlying: GenericColVector[Option[BytePointer]]) {

  def toVeColVector()(implicit
    veProcess: VeProcess,
    _source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColVector =
    VeColVector(
      transferBuffersToVe()
        .map(_.getOrElse(-1))
    ).newContainer()

  def transferBuffersToVe()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): GenericColVector[Option[Long]] = {
    measureRunningTime(
      underlying
        .map(_.map(bp => veProcess.putPointer(bp)))
        .copy(source = source)
    )(registerTransferTime)
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
      case VeScalarType.VeNullableDouble =>
        val float8Vector = new Float8Vector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 8
          float8Vector.setValueCount(numItems)
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            float8Vector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(
            bytePointersAddresses(0),
            float8Vector.getDataBufferAddress,
            dataSize
          )
        }
        float8Vector
      case VeScalarType.VeNullableLong =>
        val bigIntVector = new BigIntVector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 8
          bigIntVector.setValueCount(numItems)
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            bigIntVector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(
            bytePointersAddresses(0),
            bigIntVector.getDataBufferAddress,
            dataSize
          )
        }
        bigIntVector
      case VeScalarType.VeNullableInt =>
        val intVector = new IntVector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 4
          intVector.setValueCount(numItems)
          getUnsafe.copyMemory(
            bytePointersAddresses(1),
            intVector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(bytePointersAddresses(0), intVector.getDataBufferAddress, dataSize)
        }
        intVector
      case VeString =>
        val vcvr = new VarCharVector(underlying.name, bufferAllocator)
        if (numItems > 0) {
          val offsetsSize = (numItems + 1) * 4
          val lastOffsetIndex = numItems * 4
          val offTarget = buffers(1).get
          val dataSize = offTarget.getInt(lastOffsetIndex)
          offTarget.position(0)
          vcvr.allocateNew(dataSize, numItems)
          vcvr.setValueCount(numItems)

          getUnsafe.copyMemory(
            bytePointersAddresses(2),
            vcvr.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(bytePointersAddresses(1), vcvr.getOffsetBufferAddress, offsetsSize)
          getUnsafe.copyMemory(bytePointersAddresses(0), vcvr.getDataBufferAddress, dataSize)
        }
        vcvr
      case other => sys.error(s"Not supported for conversion to arrow vector: $other")
    }
  }
}

object BytePointerColVector {

  def fromArrowVector(
    valueVector: ValueVector
  )(implicit source: VeColVectorSource): BytePointerColVector =
    valueVector match {
      case float8Vector: Float8Vector     => fromFloat8Vector(float8Vector)
      case bigIntVector: BigIntVector     => fromBigIntVector(bigIntVector)
      case intVector: IntVector           => fromIntVector(intVector)
      case varCharVector: VarCharVector   => fromVarcharVector(varCharVector)
      case dateDayVector: DateDayVector   => fromDateDayVector(dateDayVector)
      case smallIntVector: SmallIntVector => fromSmallIntVector(smallIntVector)
      case other                          => sys.error(s"Not supported to convert from ${other.getClass}")
    }

  def fromBigIntVector(
    bigIntVector: BigIntVector
  )(implicit source: VeColVectorSource): BytePointerColVector =
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = bigIntVector.getValueCount,
        name = bigIntVector.getName,
        veType = VeScalarType.VeNullableLong,
        container = None,
        buffers = List(
          Option(new BytePointer(bigIntVector.getDataBuffer.nioBuffer())),
          Option(new BytePointer(bigIntVector.getValidityBuffer.nioBuffer()))
        ),
        variableSize = None
      )
    )

  def fromIntVector(dirInt: IntVector)(implicit source: VeColVectorSource): BytePointerColVector =
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = dirInt.getValueCount,
        name = dirInt.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(new BytePointer(dirInt.getDataBuffer.nioBuffer())),
          Option(new BytePointer(dirInt.getValidityBuffer.nioBuffer()))
        ),
        variableSize = None
      )
    )

  def fromSmallIntVector(
    smallDirInt: SmallIntVector
  )(implicit source: VeColVectorSource): BytePointerColVector = {
    val intVector = smallDirInt.toIntVector
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = smallDirInt.getValueCount,
        name = smallDirInt.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(new BytePointer(intVector.getDataBuffer.nioBuffer())),
          Option(new BytePointer(intVector.getValidityBuffer.nioBuffer()))
        ),
        variableSize = None
      )
    )
  }

  def fromDateDayVector(
    dateDayVector: DateDayVector
  )(implicit source: VeColVectorSource): BytePointerColVector =
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = dateDayVector.getValueCount,
        name = dateDayVector.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(new BytePointer(dateDayVector.getDataBuffer.nioBuffer())),
          Option(new BytePointer(dateDayVector.getValidityBuffer.nioBuffer()))
        ),
        variableSize = None
      )
    )

  def fromFloat8Vector(
    float8Vector: Float8Vector
  )(implicit source: VeColVectorSource): BytePointerColVector =
    BytePointerColVector(
      GenericColVector(
        source = source,
        numItems = float8Vector.getValueCount,
        name = float8Vector.getName,
        veType = VeScalarType.VeNullableDouble,
        container = None,
        buffers = List(
          Option(new BytePointer(float8Vector.getDataBuffer.nioBuffer())),
          Option(new BytePointer(float8Vector.getValidityBuffer.nioBuffer()))
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
    println(s"DATA SIZE CODE:${data.limit()}")
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

}
