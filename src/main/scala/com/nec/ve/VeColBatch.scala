package com.nec.ve

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
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.SparkExpressionToCExpression.likelySparkType
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.spark.planning.VeColColumnarVector
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  DateDayVector,
  FieldVector,
  Float8Vector,
  IntVector,
  SmallIntVector,
  ValueVector,
  VarCharVector
}

import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer
import java.nio.ByteBuffer

import org.apache.spark.sql.util.ArrowUtilsExposed.RichSmallIntVector

//noinspection AccessorLikeMethodIsEmptyParen
final case class VeColBatch(numRows: Int, cols: List[VeColVector]) {
  def toArrowColumnarBatch()(implicit
    bufferAllocator: BufferAllocator,
    veProcess: VeProcess
  ): ColumnarBatch = {
    val vecs = cols.map(_.toArrowVector())
    val cb = new ColumnarBatch(vecs.map(col => new ArrowColumnVector(col)).toArray)
    cb.setNumRows(numRows)
    cb
  }

  def toInternalColumnarBatch(): ColumnarBatch = {
    val vecs = cols.map(_.toInternalVector())
    val cb = new ColumnarBatch(vecs.toArray)
    cb.setNumRows(numRows)
    cb
  }
}

object VeColBatch {
  def fromList(lv: List[VeColVector]): VeColBatch = {
    assert(!lv.isEmpty)
    VeColBatch(numRows = lv.head.numItems, lv)
  }

  def empty: VeColBatch = {
    VeColBatch(0, List.empty)
  }
  final case class ColumnGroup(veType: VeType, relatedColumns: List[VeColVector]) {}

  final case class VeBatchOfBatches(cols: Int, rows: Int, batches: List[VeColBatch]) {
    def isEmpty: Boolean = !nonEmpty
    def nonEmpty: Boolean = rows > 0

    /** Transpose to get the columns from each batch aligned, ie [[1st col of 1st batch, 1st col of 2nd batch, ...], [2nd col of 1st batch, ...] */
    def groupedColumns: List[ColumnGroup] = {
      if (batches.isEmpty) Nil
      else {
        batches.head.cols.zipWithIndex.map { case (vcv, idx) =>
          ColumnGroup(
            veType = vcv.veType,
            relatedColumns = batches
              .map(_.cols.apply(idx))
              .ensuring(cond = _.forall(_.veType == vcv.veType), msg = "All types should match up")
          )
        }
      }
    }
  }

  object VeBatchOfBatches {
    def fromVeColBatches(list: List[VeColBatch]): VeBatchOfBatches = {
      VeBatchOfBatches(cols = list.head.cols.size, rows = list.map(_.numRows).sum, batches = list)
    }
  }

  def fromArrowColumnarBatch(
    columnarBatch: ColumnarBatch
  )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColBatch = {
    VeColBatch(
      numRows = columnarBatch.numRows(),
      cols = (0 until columnarBatch.numCols()).map { colNo =>
        val col = columnarBatch.column(colNo)
        VeColVector.fromVectorColumn(numRows = columnarBatch.numRows(), source = col)
      }.toList
    )
  }

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  final case class VeColVectorSource(identifier: String)
  final case class VeColVector(
    source: VeColVectorSource,
    numItems: Int,
    name: String,
    variableSize: Option[Int],
    veType: VeType,
    containerLocation: Long,
    bufferLocations: List[Long]
  ) {
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
      copy(bufferLocations = newBuffers.map { ba =>
        val byteBuffer = ByteBuffer.allocateDirect(ba.length)
        byteBuffer.put(ba, 0, ba.length)
        byteBuffer.position(0)
        veProcess.putBuffer(byteBuffer)
      })

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
      bufferLocations
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

    private def newContainer()(implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): VeColVector = {
      veType match {
        case VeScalarType.VeNullableDouble =>
          val vcvr = new nullable_double_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0)
          vcvr.validityBuffer = bufferLocations(1)
          val byteBuffer = nullableDoubleVectorToByteBuffer(vcvr)

          copy(containerLocation = veProcess.putBuffer(byteBuffer))
        case VeScalarType.VeNullableInt =>
          val vcvr = new nullable_int_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0)
          vcvr.validityBuffer = bufferLocations(1)
          val byteBuffer = nullableIntVectorToByteBuffer(vcvr)

          copy(containerLocation = veProcess.putBuffer(byteBuffer))
        case VeScalarType.VeNullableLong =>
          val vcvr = new nullable_bigint_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0)
          vcvr.validityBuffer = bufferLocations(1)
          val byteBuffer = nullableBigintVectorToByteBuffer(vcvr)

          copy(containerLocation = veProcess.putBuffer(byteBuffer))
        case VeString =>
          val vcvr = new nullable_varchar_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0)
          vcvr.offsets = bufferLocations(1)
          vcvr.validityBuffer = bufferLocations(2)
          vcvr.dataSize =
            variableSize.getOrElse(sys.error("Invalid state - VeString has no variableSize"))
          val byteBuffer = nullableVarCharVectorVectorToByteBuffer(vcvr)

          copy(containerLocation = veProcess.putBuffer(byteBuffer))
        case other => sys.error(s"Other $other not supported.")
      }
    }.copy(source = source)

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
          veProcess.get(bufferLocations.head, vhTarget, vhTarget.limit())
          veProcess.get(bufferLocations(1), validityTarget, validityTarget.limit())
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
          veProcess.get(bufferLocations.head, vhTarget, vhTarget.limit())
          veProcess.get(bufferLocations(1), validityTarget, validityTarget.limit())
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
          veProcess.get(bufferLocations.head, vhTarget, vhTarget.limit())
          veProcess.get(bufferLocations(1), validityTarget, validityTarget.limit())
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

          veProcess.get(bufferLocations(1), offTarget, offTarget.limit())
          veProcess.get(bufferLocations(2), validityTarget, validityTarget.limit())
          val dataSize = Integer.reverseBytes(offTarget.getInt(lastOffsetIndex))
          val vhTarget = ByteBuffer.allocateDirect(dataSize)

          offTarget.rewind()
          veProcess.get(bufferLocations.head, vhTarget, vhTarget.limit())
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

    def free()(implicit veProcess: VeProcess): Unit =
      (containerLocation :: bufferLocations).foreach(veProcess.free)

  }

  //noinspection ScalaUnusedSymbol
  object VeColVector {

    def fromVectorColumn(numRows: Int, source: ColumnVector)(implicit
      veProcess: VeProcess,
      _source: VeColVectorSource
    ): VeColVector = fromArrowVector(source.getArrowValueVector)

    def fromArrowVector(
      valueVector: ValueVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector =
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
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val vcvr = new nullable_bigint_vector()
      vcvr.count = bigIntVector.getValueCount
      vcvr.data = veProcess.putBuffer(bigIntVector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(bigIntVector.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableBigintVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        source = source,
        numItems = bigIntVector.getValueCount,
        name = bigIntVector.getName,
        veType = VeScalarType.VeNullableLong,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer),
        variableSize = None
      )
    }

    def fromIntVector(
      dirInt: IntVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val vcvr = new nullable_int_vector()
      vcvr.count = dirInt.getValueCount
      vcvr.data = veProcess.putBuffer(dirInt.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(dirInt.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableIntVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        source = source,
        numItems = dirInt.getValueCount,
        name = dirInt.getName,
        veType = VeScalarType.VeNullableInt,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer),
        variableSize = None
      )
    }

    def fromSmallIntVector(
      smallDirInt: SmallIntVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val intVector = smallDirInt.toIntVector
      val vcvr = new nullable_int_vector()
      vcvr.count = smallDirInt.getValueCount
      vcvr.data = veProcess.putBuffer(intVector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(intVector.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableIntVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        source = source,
        numItems = smallDirInt.getValueCount,
        name = smallDirInt.getName,
        veType = VeScalarType.VeNullableInt,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer),
        variableSize = None
      )
    }

    def fromDateDayVector(
      dateDayVector: DateDayVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val vcvr = new nullable_int_vector()
      vcvr.count = dateDayVector.getValueCount
      vcvr.data = veProcess.putBuffer(dateDayVector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(dateDayVector.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableIntVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        source = source,
        numItems = dateDayVector.getValueCount,
        name = dateDayVector.getName,
        veType = VeScalarType.VeNullableInt,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer),
        variableSize = None
      )
    }

    def fromFloat8Vector(
      float8Vector: Float8Vector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val vcvr = new nullable_double_vector()
      vcvr.count = float8Vector.getValueCount
      vcvr.data = veProcess.putBuffer(float8Vector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(float8Vector.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableDoubleVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        source = source,
        numItems = float8Vector.getValueCount,
        name = float8Vector.getName,
        veType = VeScalarType.VeNullableDouble,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer),
        variableSize = None
      )
    }

    def fromVarcharVector(
      varcharVector: VarCharVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val vcvr = new nullable_varchar_vector()
      vcvr.count = varcharVector.getValueCount
      vcvr.data = veProcess.putBuffer(varcharVector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(varcharVector.getValidityBuffer.nioBuffer())
      vcvr.offsets = veProcess.putBuffer(varcharVector.getOffsetBuffer.nioBuffer())
      vcvr.dataSize = varcharVector.sizeOfValueBuffer()
      val byteBuffer = nullableVarCharVectorVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        source = source,
        numItems = varcharVector.getValueCount,
        name = varcharVector.getName,
        veType = VeString,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.offsets, vcvr.validityBuffer),
        variableSize = Some(varcharVector.getDataBuffer.nioBuffer().capacity())
      )
    }
  }

}
