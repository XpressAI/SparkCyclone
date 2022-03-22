package com.nec.ve.colvector

import com.nec.arrow.ArrowTransferStructures._
import com.nec.arrow.colvector.{BytePointerColVector, GenericColVector, UnitColVector}
import com.nec.cache.VeColColumnarVector
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.SparkExpressionToCExpression.likelySparkType
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.nec.ve.{VeProcess, VeProcessMetrics}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.vectorized.ColumnVector
import org.bytedeco.javacpp.{BytePointer, IntPointer, LongPointer, Pointer}
import sun.misc.Unsafe

import java.io.OutputStream

final case class VeColVector(underlying: GenericColVector[Long]) {
  def serializedSize: Int = underlying.bufferSizes.sum

  def serializeToStream(outStream: OutputStream)(implicit veProcess: VeProcess): Unit =
    underlying.buffers.zip(underlying.bufferSizes).foreach { case (bufPos, bufLen) =>
      veProcess.writeToStream(outStream, bufPos, bufLen)
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
  def serialize()(implicit veProcess: VeProcess, cycloneMetrics: VeProcessMetrics): Array[Byte] = {
    val totalSize = bufferSizes.sum

    val resultingArray = cycloneMetrics.measureRunningTime(
      toBytePointerVector()
        .toByteArrayColVector()
        .serialize()
    )(cycloneMetrics.registerSerializationTime)

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

  def newContainer()(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColVector =
    copy(underlying = {
      veType match {
        case _: VeScalarType =>
          // todo replace without using JNA at all
          val double_vector = new nullable_double_vector()
          val v_bb = double_vector.getPointer.getByteBuffer(0, 20)
          v_bb.putLong(0, buffers(0))
          v_bb.putLong(8, buffers(1))
          v_bb.putInt(16, numItems)
          val bytePointer = new BytePointer(v_bb)
          underlying.copy(container = veProcess.putPointer(bytePointer))
        case VeString =>
          // todo use to replace without JNA at all
          val vcvr = new nullable_varchar_vector()
          vcvr.count = numItems
          vcvr.data = buffers(0)
          vcvr.offsets = buffers(1)
          vcvr.lengths = buffers(2)
          vcvr.validityBuffer = buffers(3)
          vcvr.dataSize =
            variableSize.getOrElse(sys.error("Invalid state - VeString has no variableSize"))

          val bytePointer = {
            val v_bb = vcvr.getPointer.getByteBuffer(0, (8 * 4) + (4 * 2))
            v_bb.putLong(0, vcvr.data)
            v_bb.putLong(8, vcvr.offsets)
            v_bb.putLong(16, vcvr.lengths)
            v_bb.putLong(24, vcvr.validityBuffer)
            v_bb.putInt(32, vcvr.dataSize)
            v_bb.putInt(36, vcvr.count)
            new BytePointer(v_bb)
          }

          underlying.copy(container = veProcess.putPointer(bytePointer))
        case other => sys.error(s"Other $other not supported.")
      }
    }.copy(source = source))

  def containerSize: Int = veType.containerSize

  def toArrowVector()(implicit
    veProcess: VeProcess,
    bufferAllocator: BufferAllocator
  ): FieldVector = toBytePointerVector().toArrowVector()

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
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColVector = fromArrowVector(source.getArrowValueVector)

  def fromArrowVector(valueVector: ValueVector)(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColVector =
    BytePointerColVector
      .fromArrowVector(valueVector)
      .toVeColVector()


  def fromPointer(pointer: Pointer)(implicit
                                       veProcess: VeProcess,
                                       source: VeColVectorSource,
                                       originalCallingContext: OriginalCallingContext,
                                       cycloneMetrics: VeProcessMetrics
  ): VeColVector = {
    val size = Math.ceil(pointer.limit() / 64).toLong
    val lp = new LongPointer(size.toLong)
    for (i <- 0 until size.toInt) {
      lp.put(i, -1)
    }
    BytePointerColVector.fromPointer(pointer, lp)
      .toVeColVector()
  }
}
