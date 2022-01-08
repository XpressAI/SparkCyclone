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
import org.apache.spark.sql.util.ArrowUtilsExposed.RichSmallIntVector
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

  if (veType == VeString) require(variableSize.nonEmpty, "String should come with variable size")

  /**
   * Sizes of the underlying buffers --- use veType & combination with numItmes to decide them.
   */
  def bufferSizes: List[Int] = veType match {
    case v: VeScalarType => List(numItems * v.cSize, Math.ceil(numItems / 64.0).toInt * 8)
    case VeString =>
      val offsetBuffSize = (numItems + 1) * 4
      val validitySize = Math.ceil(numItems / 64.0).toInt * 8

      variableSize.toList ++ List(offsetBuffSize, validitySize)
  }

  def injectBuffers(newBuffers: List[Array[Byte]])(implicit veProcess: VeProcess): VeColVector =
    copy(underlying = underlying.copy(buffers = newBuffers.map { ba =>
      val byteBuffer = ByteBuffer.allocateDirect(ba.length)
      byteBuffer.put(ba, 0, ba.length)
      byteBuffer.position(0)
      veProcess.putBuffer(byteBuffer)
    }))

  /**
   * Retrieve data from veProcess, put it into a Byte Array. Uses bufferSizes.
   */
  def serialize()(implicit veProcess: VeProcess): Array[Byte] = {
    val totalSize = bufferSizes.sum

    val extractedBuffers = extractBuffers()

    val resultingArray = Array.ofDim[Byte](totalSize)
    val bufferStarts = extractedBuffers.map(_.length).scanLeft(0)(_ + _)
    bufferStarts.zip(extractedBuffers).foreach { case (start, buffer) =>
      System.arraycopy(buffer, 0, resultingArray, start, buffer.length)
    }

    assert(
      resultingArray.length == totalSize,
      "Resulting array should be same size as sum of all buffer sizes"
    )

    resultingArray
  }

  def extractBuffers()(implicit veProcess: VeProcess): List[Array[Byte]] = {
    buffers
      .zip(bufferSizes)
      .map { case (veBufferLocation, veBufferSize) =>
        val targetBuf = ByteBuffer.allocateDirect(veBufferSize)
        veProcess.get(veBufferLocation, targetBuf, veBufferSize)
        val dst = Array.fill[Byte](veBufferSize)(-1)
        targetBuf.get(dst, 0, veBufferSize)
        dst
      }
  }

  /**
   * Decompose the Byte Array and allocate into VeProcess. Uses bufferSizes.
   *
   * The parent ColVector is a description of the original source vector from another VE that
   * could be on an entirely separate machine. Here, by deserializing, we allocate one on our specific VE process.
   */
  def deserialize(
    ba: Array[Byte]
  )(implicit source: VeColVectorSource, veProcess: VeProcess): VeColVector =
    injectBuffers(newBuffers =
      bufferSizes.scanLeft(0)(_ + _).zip(bufferSizes).map { case (bufferStart, bufferSize) =>
        ba.slice(bufferStart, bufferStart + bufferSize)
      }
    ).newContainer()

  def newContainer()(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector =
    copy(underlying = {
      veType match {
        case VeScalarType.VeNullableDouble =>
          val vcvr = new nullable_double_vector()
          vcvr.count = numItems
          vcvr.data = buffers(0)
          vcvr.validityBuffer = buffers(1)
          val byteBuffer = nullableDoubleVectorToByteBuffer(vcvr)

          underlying.copy(container = veProcess.putBuffer(byteBuffer))
        case VeScalarType.VeNullableInt =>
          val vcvr = new nullable_int_vector()
          vcvr.count = numItems
          vcvr.data = buffers(0)
          vcvr.validityBuffer = buffers(1)
          val byteBuffer = nullableIntVectorToByteBuffer(vcvr)

          underlying.copy(container = veProcess.putBuffer(byteBuffer))
        case VeScalarType.VeNullableLong =>
          val vcvr = new nullable_bigint_vector()
          vcvr.count = numItems
          vcvr.data = buffers(0)
          vcvr.validityBuffer = buffers(1)
          val byteBuffer = nullableBigintVectorToByteBuffer(vcvr)

          underlying.copy(container = veProcess.putBuffer(byteBuffer))
        case VeString =>
          val vcvr = new nullable_varchar_vector()
          vcvr.count = numItems
          vcvr.data = buffers(0)
          vcvr.offsets = buffers(1)
          vcvr.validityBuffer = buffers(2)
          vcvr.dataSize =
            variableSize.getOrElse(sys.error("Invalid state - VeString has no variableSize"))
          val byteBuffer = nullableVarCharVectorVectorToByteBuffer(vcvr)

          underlying.copy(container = veProcess.putBuffer(byteBuffer))
        case other => sys.error(s"Other $other not supported.")
      }
    }.copy(source = source))

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
