package com.nec.ve.colvector

import com.nec.arrow.ArrowTransferStructures.{
  nullable_bigint_vector,
  nullable_double_vector,
  nullable_int_vector,
  nullable_varchar_vector
}
import com.nec.arrow.VeArrowTransfers.{
  nullableBigintVectorToByteBuffer,
  nullableDoubleVectorToByteBuffer,
  nullableIntVectorToByteBuffer,
  nullableVarCharVectorVectorToByteBuffer
}
import com.nec.arrow.colvector.{ByteBufferColVector, GenericColVector}
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.SparkExpressionToCExpression.likelySparkType
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.spark.planning.VeColColumnarVector
import com.nec.ve.VeProcess
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector.getUnsafe
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.vectorized.ColumnVector
import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer

import java.nio.ByteBuffer

final case class VeColVector(underlying: GenericColVector[Long]) {
  def bufferLocations = underlying.buffers
  def containerLocation = underlying.containerLocation
  def source = underlying.source
  def numItems = underlying.numItems
  def name = underlying.name
  def variableSize = underlying.variableSize
  def veType = underlying.veType
  def buffers = underlying.buffers

  import underlying._
  def toInternalVector(): ColumnVector = new VeColColumnarVector(this, likelySparkType(veType))

  def nonEmpty: Boolean = numItems > 0
  def isEmpty: Boolean = !nonEmpty

  /**
   * Retrieve data from veProcess, put it into a Byte Array. Uses bufferSizes.
   */
  def serialize()(implicit veProcess: VeProcess): Array[Byte] = {
    val bufsizes = bufferSizes
    val maxBufSize = if (bufsizes.size > 0) bufsizes.max else -1

    // Allocate ByteBuffer just once
    val tmpbuffer = ByteBuffer.allocateDirect(maxBufSize)
    val output = Array.fill[Byte](bufsizes.sum)(-1)

    (buffers, bufsizes.scanLeft(0)(_ + _), bufsizes).zipped
      .foreach { case (vebuffer, offset, size) =>
        // Reset the ByteBuffer's current position
        tmpbuffer.position(0)
        // Fetch from VE to ByteBuffer
        veProcess.get(vebuffer, tmpbuffer, size)
        // Copy from ByteBuffer to Array[Byte] at offset, starting from ByteBuffer's current position
        tmpbuffer.get(output, offset, size)
      }

    output
  }

  def toByteBufferVector()(implicit veProcess: VeProcess): ByteBufferColVector =
    ByteBufferColVector(
      underlying.copy(
        container = None,
        buffers = {
          buffers
            .zip(bufferSizes)
            .map { case (veBufferLocation, veBufferSize) =>
              val targetBuf = ByteBuffer.allocateDirect(veBufferSize)
              veProcess.get(veBufferLocation, targetBuf, veBufferSize)
              Option(targetBuf)
            }
        }
      )
    )

  def newContainer()(implicit source: VeColVectorSource, veProcess: VeProcess): VeColVector = {
    val container = veType match {
      case VeScalarType.VeNullableDouble =>
        val vcvr = new nullable_double_vector()
        vcvr.count = numItems
        vcvr.data = bufferLocations(0)
        vcvr.validityBuffer = bufferLocations(1)
        veProcess.putBuffer(nullableDoubleVectorToByteBuffer(vcvr))

      case VeScalarType.VeNullableInt =>
        val vcvr = new nullable_int_vector()
        vcvr.count = numItems
        vcvr.data = bufferLocations(0)
        vcvr.validityBuffer = bufferLocations(1)
        veProcess.putBuffer(nullableIntVectorToByteBuffer(vcvr))

      case VeScalarType.VeNullableLong =>
        val vcvr = new nullable_bigint_vector()
        vcvr.count = numItems
        vcvr.data = bufferLocations(0)
        vcvr.validityBuffer = bufferLocations(1)
        veProcess.putBuffer(nullableBigintVectorToByteBuffer(vcvr))

      case VeString =>
        val vcvr = new nullable_varchar_vector()
        vcvr.count = numItems
        vcvr.data = bufferLocations(0)
        vcvr.offsets = bufferLocations(1)
        vcvr.validityBuffer = bufferLocations(2)
        vcvr.dataSize = variableSize.getOrElse(sys.error("Invalid state - VeString has no variableSize"))
        veProcess.putBuffer(nullableVarCharVectorVectorToByteBuffer(vcvr))

      case other =>
        sys.error(s"VeType ${other} is not supported.")
    }

    copy(underlying = underlying.copy(container = container, source = source))
  }

  def containerSize: Int = veType.containerSize

  def toArrowVector()(implicit
    veProcess: VeProcess,
    bufferAllocator: BufferAllocator
  ): FieldVector = veType match {
    case VeScalarType.VeNullableDouble =>
      val float8Vector = new Float8Vector("output", bufferAllocator)
      if (numItems > 0) {
        val dataSize = numItems * 8
        float8Vector.setValueCount(numItems)
        val vhTarget = ByteBuffer.allocateDirect(dataSize)
        val validityTarget = ByteBuffer.allocateDirect(numItems)
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        veProcess.get(buffers(1), validityTarget, validityTarget.limit())
        getUnsafe.copyMemory(
          validityTarget.asInstanceOf[DirectBuffer].address(),
          float8Vector.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(
          vhTarget.asInstanceOf[DirectBuffer].address(),
          float8Vector.getDataBufferAddress,
          dataSize
        )
      }
      float8Vector
    case VeScalarType.VeNullableLong =>
      val bigIntVector = new BigIntVector("output", bufferAllocator)
      if (numItems > 0) {
        val dataSize = numItems * 8
        bigIntVector.setValueCount(numItems)
        val vhTarget = ByteBuffer.allocateDirect(dataSize)
        val validityTarget = ByteBuffer.allocateDirect(numItems)
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        veProcess.get(buffers(1), validityTarget, validityTarget.limit())
        getUnsafe.copyMemory(
          validityTarget.asInstanceOf[DirectBuffer].address(),
          bigIntVector.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(
          vhTarget.asInstanceOf[DirectBuffer].address(),
          bigIntVector.getDataBufferAddress,
          dataSize
        )
      }
      bigIntVector
    case VeScalarType.VeNullableInt =>
      val intVector = new IntVector("output", bufferAllocator)
      if (numItems > 0) {
        val dataSize = numItems * 4
        intVector.setValueCount(numItems)
        val vhTarget = ByteBuffer.allocateDirect(dataSize)
        val validityTarget = ByteBuffer.allocateDirect(numItems)
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        veProcess.get(buffers(1), validityTarget, validityTarget.limit())
        getUnsafe.copyMemory(
          validityTarget.asInstanceOf[DirectBuffer].address(),
          intVector.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(
          vhTarget.asInstanceOf[DirectBuffer].address(),
          intVector.getDataBufferAddress,
          dataSize
        )
      }
      intVector
    case VeString =>
      val vcvr = new VarCharVector("output", bufferAllocator)
      if (numItems > 0) {
        val offsetsSize = (numItems + 1) * 4
        val lastOffsetIndex = numItems * 4
        val offTarget = ByteBuffer.allocateDirect(offsetsSize)
        val validityTarget = ByteBuffer.allocateDirect(numItems)

        veProcess.get(buffers(1), offTarget, offTarget.limit())
        veProcess.get(buffers(2), validityTarget, validityTarget.limit())
        val dataSize = Integer.reverseBytes(offTarget.getInt(lastOffsetIndex))
        val vhTarget = ByteBuffer.allocateDirect(dataSize)

        offTarget.rewind()
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        vcvr.allocateNew(dataSize, numItems)
        vcvr.setValueCount(numItems)

        getUnsafe.copyMemory(
          validityTarget.asInstanceOf[DirectBuffer].address(),
          vcvr.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(
          offTarget.asInstanceOf[DirectBuffer].address(),
          vcvr.getOffsetBufferAddress,
          offsetsSize
        )
        getUnsafe.copyMemory(
          vhTarget.asInstanceOf[DirectBuffer].address(),
          vcvr.getDataBufferAddress,
          dataSize
        )
      }
      vcvr
    case other => sys.error(s"Not supported for conversion to arrow vector: $other")
  }

  def free()(implicit veProcess: VeProcess, veColVectorSource: VeColVectorSource): Unit = {
    require(
      veColVectorSource == source,
      s"Intended to `free` in ${source}, but got ${veColVectorSource} context."
    )
    (containerLocation :: buffers).foreach(veProcess.free)
  }

}

//noinspection ScalaUnusedSymbol
object VeColVector {

  def apply(
    source: VeColVectorSource,
    numItems: Int,
    name: String,
    variableSize: Option[Int],
    veType: VeType,
    containerLocation: Long,
    bufferLocations: List[Long]
  ): VeColVector = VeColBatch.VeColVector(
    GenericColVector[Long](
      source = source,
      numItems = numItems,
      name = name,
      variableSize = variableSize,
      veType = veType,
      container = containerLocation,
      buffers = bufferLocations
    )
  )

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def fromVectorColumn(numRows: Int, source: ColumnVector)(implicit
    veProcess: VeProcess,
    _source: VeColVectorSource
  ): VeColVector = fromArrowVector(source.getArrowValueVector)

  def fromArrowVector(
    valueVector: ValueVector
  )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector =
    ByteBufferColVector
      .fromArrowVector(valueVector)
      .toVeColVector()

}
