package com.nec.arrow.colvector

import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString}
import com.nec.spark.planning.Tracer.TracerVector.veType
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector
import com.nec.ve.colvector.VeColVector.getUnsafe
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtilsExposed.RichSmallIntVector
import sun.nio.ch.DirectBuffer
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerTransferTime}
import java.nio.ByteBuffer

/**
 * Storage of a col vector as serialized Arrow buffers, that are in ByteBuffers.
 * We use Option[] because the `container` has no ByteBuffer.
 */
final case class ByteBufferColVector(underlying: GenericColVector[Option[ByteBuffer]]) {

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
        .map(_.map(bb => veProcess.putBuffer(bb)))
        .copy(source = source)
    )(registerTransferTime)
  }

  def toByteArrayColVector(): ByteArrayColVector =
    ByteArrayColVector(
      underlying.copy(
        container = None,
        buffers = underlying
          .map(_.map(bb => {
            try bb.array()
            catch {
              case _: UnsupportedOperationException =>
                val size = bb.capacity()
                val target: Array[Byte] = Array.fill(size)(-1)
                bb.get(target)
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
    val byteBuffersAddresses = buffers.flatten.map(_.asInstanceOf[DirectBuffer].address())
    veType match {
      case VeScalarType.VeNullableDouble =>
        val float8Vector = new Float8Vector("output", bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 8
          float8Vector.setValueCount(numItems)
          getUnsafe.copyMemory(
            byteBuffersAddresses(1),
            float8Vector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(byteBuffersAddresses(0), float8Vector.getDataBufferAddress, dataSize)
        }
        float8Vector
      case VeScalarType.VeNullableLong =>
        val bigIntVector = new BigIntVector("output", bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 8
          bigIntVector.setValueCount(numItems)
          getUnsafe.copyMemory(
            byteBuffersAddresses(1),
            bigIntVector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(byteBuffersAddresses(0), bigIntVector.getDataBufferAddress, dataSize)
        }
        bigIntVector
      case VeScalarType.VeNullableInt =>
        val intVector = new IntVector("output", bufferAllocator)
        if (numItems > 0) {
          val dataSize = numItems * 4
          intVector.setValueCount(numItems)
          getUnsafe.copyMemory(
            byteBuffersAddresses(1),
            intVector.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(byteBuffersAddresses(0), intVector.getDataBufferAddress, dataSize)
        }
        intVector
      case VeString =>
        val vcvr = new VarCharVector("output", bufferAllocator)
        if (numItems > 0) {
          val offsetsSize = (numItems + 1) * 4
          val lastOffsetIndex = numItems * 4
          val offTarget = buffers(1).get
          val dataSize = Integer.reverseBytes(offTarget.getInt(lastOffsetIndex))
          offTarget.rewind()
          vcvr.allocateNew(dataSize, numItems)
          vcvr.setValueCount(numItems)

          getUnsafe.copyMemory(
            byteBuffersAddresses(2),
            vcvr.getValidityBufferAddress,
            Math.ceil(numItems / 64.0).toInt * 8
          )
          getUnsafe.copyMemory(byteBuffersAddresses(1), vcvr.getOffsetBufferAddress, offsetsSize)
          getUnsafe.copyMemory(byteBuffersAddresses(0), vcvr.getDataBufferAddress, dataSize)
        }
        vcvr
      case other => sys.error(s"Not supported for conversion to arrow vector: $other")
    }
  }
}

object ByteBufferColVector {

  def fromArrowVector(
    valueVector: ValueVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
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
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = bigIntVector.getValueCount,
        name = bigIntVector.getName,
        veType = VeScalarType.VeNullableLong,
        container = None,
        buffers = List(
          Option(bigIntVector.getDataBuffer.nioBuffer()),
          Option(bigIntVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromIntVector(dirInt: IntVector)(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = dirInt.getValueCount,
        name = dirInt.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(dirInt.getDataBuffer.nioBuffer()),
          Option(dirInt.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromSmallIntVector(
    smallDirInt: SmallIntVector
  )(implicit source: VeColVectorSource): ByteBufferColVector = {
    val intVector = smallDirInt.toIntVector
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = smallDirInt.getValueCount,
        name = smallDirInt.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(intVector.getDataBuffer.nioBuffer()),
          Option(intVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )
  }

  def fromDateDayVector(
    dateDayVector: DateDayVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = dateDayVector.getValueCount,
        name = dateDayVector.getName,
        veType = VeScalarType.VeNullableInt,
        container = None,
        buffers = List(
          Option(dateDayVector.getDataBuffer.nioBuffer()),
          Option(dateDayVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromFloat8Vector(
    float8Vector: Float8Vector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = float8Vector.getValueCount,
        name = float8Vector.getName,
        veType = VeScalarType.VeNullableDouble,
        container = None,
        buffers = List(
          Option(float8Vector.getDataBuffer.nioBuffer()),
          Option(float8Vector.getValidityBuffer.nioBuffer())
        ),
        variableSize = None
      )
    )

  def fromVarcharVector(
    varcharVector: VarCharVector
  )(implicit source: VeColVectorSource): ByteBufferColVector =
    ByteBufferColVector(
      GenericColVector(
        source = source,
        numItems = varcharVector.getValueCount,
        name = varcharVector.getName,
        veType = VeString,
        container = None,
        buffers = List(
          Option(varcharVector.getDataBuffer.nioBuffer()),
          Option(varcharVector.getOffsetBuffer.nioBuffer()),
          Option(varcharVector.getValidityBuffer.nioBuffer())
        ),
        variableSize = Some(varcharVector.getDataBuffer.nioBuffer().capacity())
      )
    )

}
