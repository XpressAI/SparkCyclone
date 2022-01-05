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
import com.nec.ve.ColVector.MaybeByteArrayColVector
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  FieldVector,
  Float8Vector,
  IntVector,
  ValueVector,
  VarCharVector
}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer

import java.nio.ByteBuffer

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
    assert(lv.nonEmpty)
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

  final case class VectorEngineLocation(value: Long)
  type VeColVector = ColVector[VectorEngineLocation]

  implicit class RichVeColVector(cv: ColVector[VectorEngineLocation]) {
    import cv._

    def toInternalVector(): ColumnVector = new VeColColumnarVector(cv, likelySparkType(veType))

    def toByteArrayContainer()(implicit veProcess: VeProcess): MaybeByteArrayColVector = {
      copy(
        bufferLocations = bufferLocations
          .zip(bufferSizes)
          .map { case (veBufferLocation, veBufferSize) =>
            val targetBuf = ByteBuffer.allocateDirect(veBufferSize)
            veProcess.get(veBufferLocation.value, targetBuf, veBufferSize)
            val dst = Array.fill[Byte](veBufferSize)(-1)
            targetBuf.get(dst, 0, veBufferSize)
            Option(dst)
          },
        containerLocation = None
      )
    }

    def extractBuffers()(implicit veProcess: VeProcess): List[Array[Byte]] =
      toByteArrayContainer.bufferLocations.flatten

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

    private[ve] def injectBuffers(
      newBuffers: List[Array[Byte]]
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      val withBuffers: MaybeByteArrayColVector =
        copy(containerLocation = Option.empty, bufferLocations = newBuffers.map(b => Option(b)))
      import ColVector._
      withBuffers.transferBuffersToVe().map(_.getOrElse(VectorEngineLocation(-1)))
    }

    private[ve] def newContainer()(implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): VeColVector = {
      veType match {
        case VeScalarType.VeNullableDouble =>
          val vcvr = new nullable_double_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0).value
          vcvr.validityBuffer = bufferLocations(1).value
          val byteBuffer = nullableDoubleVectorToByteBuffer(vcvr)

          copy(containerLocation = VectorEngineLocation(veProcess.putBuffer(byteBuffer)))
        case VeScalarType.VeNullableInt =>
          val vcvr = new nullable_int_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0).value
          vcvr.validityBuffer = bufferLocations(1).value
          val byteBuffer = nullableIntVectorToByteBuffer(vcvr)

          copy(containerLocation = VectorEngineLocation(veProcess.putBuffer(byteBuffer)))
        case VeScalarType.VeNullableLong =>
          val vcvr = new nullable_bigint_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0).value
          vcvr.validityBuffer = bufferLocations(1).value
          val byteBuffer = nullableBigintVectorToByteBuffer(vcvr)

          copy(containerLocation = VectorEngineLocation(veProcess.putBuffer(byteBuffer)))
        case VeString =>
          val vcvr = new nullable_varchar_vector()
          vcvr.count = numItems
          vcvr.data = bufferLocations(0).value
          vcvr.offsets = bufferLocations(1).value
          vcvr.validityBuffer = bufferLocations(2).value
          vcvr.dataSize =
            variableSize.getOrElse(sys.error("Invalid state - VeString has no variableSize"))
          val byteBuffer = nullableVarCharVectorVectorToByteBuffer(vcvr)

          copy(containerLocation = VectorEngineLocation(veProcess.putBuffer(byteBuffer)))
        case other => sys.error(s"Other $other not supported.")
      }
    }.copy(source = source)

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
          veProcess.get(bufferLocations.head.value, vhTarget, vhTarget.limit())
          veProcess.get(bufferLocations(1).value, validityTarget, validityTarget.limit())
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
          veProcess.get(bufferLocations.head.value, vhTarget, vhTarget.limit())
          veProcess.get(bufferLocations(1).value, validityTarget, validityTarget.limit())
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
          veProcess.get(bufferLocations.head.value, vhTarget, vhTarget.limit())
          veProcess.get(bufferLocations(1).value, validityTarget, validityTarget.limit())
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

          veProcess.get(bufferLocations(1).value, offTarget, offTarget.limit())
          veProcess.get(bufferLocations(2).value, validityTarget, validityTarget.limit())
          val dataSize = Integer.reverseBytes(offTarget.getInt(lastOffsetIndex))
          val vhTarget = ByteBuffer.allocateDirect(dataSize)

          offTarget.rewind()
          veProcess.get(bufferLocations.head.value, vhTarget, vhTarget.limit())
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
      (containerLocation :: bufferLocations).foreach(loc => veProcess.free(loc.value))
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
    ): VeColVector = ColVector[VectorEngineLocation](
      source = source,
      numItems = numItems,
      name = name,
      variableSize = variableSize,
      veType = veType,
      containerLocation = VectorEngineLocation(containerLocation),
      bufferLocations = bufferLocations.map(VectorEngineLocation)
    )

    def fromVectorColumn(numRows: Int, source: ColumnVector)(implicit
      veProcess: VeProcess,
      _source: VeColVectorSource
    ): VeColVector = fromArrowVector(source.getArrowValueVector)

    import ByteBufferVeColVector._
    def fromArrowVector(
      valueVector: ValueVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColVector = {
      ByteBufferVeColVector
        .fromArrowVector(valueVector)
        .transferBuffersToVe()
        .map(_.getOrElse(VectorEngineLocation(-1)))
        .newContainer()

    }

  }
}
