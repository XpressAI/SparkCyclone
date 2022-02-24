package com.nec.ve.colvector

import com.nec.arrow.TransferDefinitions.{
  nullable_bigint_vector,
  nullable_double_vector,
  nullable_int_vector,
  nullable_varchar_vector
}
import com.nec.arrow.ArrowInterfaces.getUnsafe
import com.nec.arrow.VeArrowTransfers.{
  nullableBigintVectorToBytePointer,
  nullableDoubleVectorToBytePointer,
  nullableIntVectorToBytePointer,
  nullableVarCharVectorVectorToBytePointer
}
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{
  measureRunningTime,
  registerSerializationTime
}
import com.nec.arrow.colvector.{BytePointerColVector, GenericColVector, UnitColVector}
import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.SparkExpressionToCExpression.likelySparkType
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeProcess
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.colvector.VeColVector.getUnsafe
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.Pointer
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.javacpp.DoublePointer
import org.bytedeco.javacpp.LongPointer
import org.bytedeco.javacpp.IntPointer
import sun.misc.Unsafe

import java.io.{DataOutputStream, OutputStream}

final case class VeColVector(underlying: GenericColVector[Long]) {
  def serializedSize: Int = underlying.bufferSizes.sum

  def serializeToStream(outStream: OutputStream)(implicit veProcess: VeProcess): Unit = {
    underlying.buffers.zip(underlying.bufferSizes).foreach { case (bufPos, bufLen) =>
      veProcess.writeToStream(outStream, bufPos, bufLen)
    }
  }

  def toUnit: UnitColVector = underlying.toUnit
  def allAllocations = containerLocation :: bufferLocations
  def bufferLocations = underlying.buffers
  def containerLocation = underlying.containerLocation
  def source = underlying.source
  def numItems = underlying.numItems
  def name = underlying.name
  def variableSize = underlying.variableSize
  def veType = underlying.veType
  def buffers = underlying.buffers

  import underlying._
  def toInternalVector(): ColumnVector =
    new VeColColumnarVector(Left(this), likelySparkType(veType))

  def nonEmpty: Boolean = numItems > 0
  def isEmpty: Boolean = !nonEmpty

  /**
   * Retrieve data from veProcess, put it into a Byte Array. Uses bufferSizes.
   */
  def serialize()(implicit veProcess: VeProcess): Array[Byte] = {
    val totalSize = bufferSizes.sum

    val resultingArray = measureRunningTime(
      toBytePointerVector()
        .toByteArrayColVector()
        .serialize()
    )(registerSerializationTime)

    assert(
      resultingArray.length == totalSize,
      "Resulting array should be same size as sum of all buffer sizes"
    )

    resultingArray
  }

  def toBytePointerVector()(implicit veProcess: VeProcess): BytePointerColVector =
    BytePointerColVector(
      underlying.copy(
        container = None,
        buffers = {
          buffers
            .zip(bufferSizes)
            .map { case (veBufferLocation, veBufferSize) =>
              val targetBuf = new BytePointer(veBufferSize)
              veProcess.get(veBufferLocation, targetBuf, veBufferSize)
              Option(targetBuf)
            }
        }
      )
    )

  class UnsafeBytePointer extends BytePointer
  {
    def setAddress(to: Long): Unit = {
      address = to
    }
  }

  class UnsafeDoublePointer extends DoublePointer
  {
    def setAddress(to: Long): Unit = {
      address = to
    }
  }

  class UnsafeIntPointer extends IntPointer
  {
    def setAddress(to: Long): Unit = {
      address = to
    }
  }

  class UnsafeLongPointer extends LongPointer
  {
    def setAddress(to: Long): Unit = {
      address = to
    }
  }

  def newContainer()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColVector =
    copy(underlying = {
      veType match {
        case VeScalarType.VeNullableDouble =>
          val ptr = (new UnsafeDoublePointer())
          ptr.setAddress(buffers(0))
          val validityPtr = (new UnsafeLongPointer())
          validityPtr.setAddress(buffers(1))
          val vcvr = new nullable_double_vector()
            .count(numItems)
            .data(ptr)
            .validityBuffer(validityPtr)
          val bytePointer = nullableDoubleVectorToBytePointer(vcvr)

          underlying.copy(container = veProcess.putPointer(bytePointer))
        case VeScalarType.VeNullableInt =>
          val ptr = (new UnsafeIntPointer())
          ptr.setAddress(buffers(0))
          val validityPtr = (new UnsafeLongPointer())
          validityPtr.setAddress(buffers(1))
          val vcvr = new nullable_int_vector()
            .count(numItems)
            .data(ptr)
            .validityBuffer(validityPtr)
          val bytePointer = nullableIntVectorToBytePointer(vcvr)

          underlying.copy(container = veProcess.putPointer(bytePointer))
        case VeScalarType.VeNullableLong =>
          val ptr = (new UnsafeLongPointer())
          ptr.setAddress(buffers(0))
          val validityPtr = (new UnsafeLongPointer())
          validityPtr.setAddress(buffers(1))
          val vcvr = new nullable_bigint_vector()
            .count(numItems)
            .data(ptr)
            .validityBuffer(validityPtr)
          val bytePointer = nullableBigintVectorToBytePointer(vcvr)

          underlying.copy(container = veProcess.putPointer(bytePointer))
        case VeString =>
          val ptr = (new UnsafeIntPointer())
          ptr.setAddress(buffers(0))
          val offsetsPtr = (new UnsafeIntPointer())
          offsetsPtr.setAddress(buffers(1))
          val lengthsPtr = (new UnsafeIntPointer())
          lengthsPtr.setAddress(buffers(2))
          val validityPtr = (new UnsafeLongPointer())
          validityPtr.setAddress(buffers(3))

          val vcvr = new nullable_varchar_vector()
            .count(numItems)
            .data(ptr)
            .offsets(offsetsPtr)
            .lengths(lengthsPtr)
            .validityBuffer(validityPtr)
            .dataSize(
            variableSize.getOrElse(sys.error("Invalid state - VeString has no variableSize")))
          val bytePointer = nullableVarCharVectorVectorToBytePointer(vcvr)

          underlying.copy(container = veProcess.putPointer(bytePointer))
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
        val vhTarget = new BytePointer(dataSize)
        val validityTarget = new BytePointer(numItems)
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        veProcess.get(buffers(1), validityTarget, validityTarget.limit())
        getUnsafe.copyMemory(
          validityTarget.address(),
          float8Vector.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(vhTarget.address(), float8Vector.getDataBufferAddress, dataSize)
      }
      float8Vector
    case VeScalarType.VeNullableLong =>
      val bigIntVector = new BigIntVector("output", bufferAllocator)
      if (numItems > 0) {
        val dataSize = numItems * 8
        bigIntVector.setValueCount(numItems)
        val vhTarget = new BytePointer(dataSize)
        val validityTarget = new BytePointer(numItems)
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        veProcess.get(buffers(1), validityTarget, validityTarget.limit())
        getUnsafe.copyMemory(
          validityTarget.address(),
          bigIntVector.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(vhTarget.address(), bigIntVector.getDataBufferAddress, dataSize)
      }
      bigIntVector
    case VeScalarType.VeNullableInt =>
      val intVector = new IntVector("output", bufferAllocator)
      if (numItems > 0) {
        val dataSize = numItems * 4
        intVector.setValueCount(numItems)
        val vhTarget = new BytePointer(dataSize)
        val validityTarget = new BytePointer(numItems)
        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        veProcess.get(buffers(1), validityTarget, validityTarget.limit())
        getUnsafe.copyMemory(
          validityTarget.address(),
          intVector.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
        getUnsafe.copyMemory(vhTarget.address(), intVector.getDataBufferAddress, dataSize)
      }
      intVector
    case VeString =>
      val vcvr = new VarCharVector("output", bufferAllocator)
      if (numItems > 0) {
        val buffersSize = numItems * 4
        val lastOffsetIndex = (numItems - 1) * 4
        val lengthTarget = new BytePointer(buffersSize)
        val startsTarget = new BytePointer(buffersSize)
        val validityTarget = new BytePointer(numItems)
        veProcess.get(buffers(1), startsTarget, startsTarget.capacity())
        veProcess.get(buffers(2), lengthTarget, lengthTarget.capacity())
        veProcess.get(buffers(3), validityTarget, validityTarget.limit())

        val dataSize = (startsTarget.getInt(lastOffsetIndex) + lengthTarget.getInt(lastOffsetIndex))
        val vhTarget = new BytePointer(dataSize * 4)

        veProcess.get(buffers.head, vhTarget, vhTarget.limit())
        vcvr.allocateNew(dataSize, numItems)
        vcvr.setValueCount(numItems)
        //TODO: tempFix
        val array = new Array[Byte](dataSize * 4)
        vhTarget.get(array)
//        for (i <- 0 until numItems) {
//          println(s"START for idx: ${i} is ${startsTarget.getInt(i * 4)}")
//          println(s"LENGTHS for idx: ${i} is ${lengthTarget.getInt(i * 4)}")
//        }
//        println("THE ARRAY:" + new String(array))
        for (i <- 0 until numItems) {
          val start = startsTarget.getInt(i * 4) * 4
          val length = lengthTarget.getInt(i * 4) * 4
//          println(s"ARRAY LENGTH WAS ${array.length}, START ${start} and END ${length}")
          val str = new String(array, start, length, "UTF-32LE")
          val utf8bytes = str.getBytes
//          println(s"DATA: ${str}")
          vcvr.set(i, utf8bytes)
        }
        getUnsafe.copyMemory(
          validityTarget.address(),
          vcvr.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
      }
      vcvr
    case other => sys.error(s"Not supported for conversion to arrow vector: $other")
  }

  def free()(implicit
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): Unit = {
    require(
      veColVectorSource == source,
      s"Intended to `free` in ${source}, but got ${veColVectorSource} context."
    )
    allAllocations.foreach(veProcess.free)
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

  def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def fromVectorColumn(numRows: Int, source: ColumnVector)(implicit
    veProcess: VeProcess,
    _source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColVector = fromArrowVector(source.getArrowValueVector)

  def fromArrowVector(valueVector: ValueVector)(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColVector =
    BytePointerColVector
      .fromArrowVector(valueVector)
      .toVeColVector()

}
