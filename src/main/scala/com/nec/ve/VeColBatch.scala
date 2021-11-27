package com.nec.ve

import com.nec.arrow.ArrowTransferStructures.{
  nullable_bigint_vector,
  nullable_double_vector,
  nullable_varchar_vector
}
import com.nec.arrow.VeArrowTransfers.{
  nullableBigintVectorToByteBuffer,
  nullableDoubleVectorToByteBuffer,
  nullableVarCharVectorVectorToByteBuffer
}
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, Float8Vector, VarCharVector}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer

import java.nio.ByteBuffer

final case class VeColBatch(numRows: Int, cols: List[VeColVector]) {
  def toArrowColumnarBatch()(implicit
    bufferAllocator: BufferAllocator,
    veProcess: VeProcess
  ): ColumnarBatch = {
    val cb = new ColumnarBatch(cols.map(col => new ArrowColumnVector(col.toArrowVector())).toArray)
    cb.setNumRows(numRows)
    cb
  }
}

object VeColBatch {

  def fromColumnarBatch(columnarBatch: ColumnarBatch)(implicit veProcess: VeProcess): VeColBatch = {
    VeColBatch(
      numRows = columnarBatch.numRows(),
      cols = (0 until columnarBatch.numCols()).map { colNo =>
        val col = columnarBatch.column(colNo)
        val sparkType = col.dataType()
        VeColVector.fromVectorColumn(numRows = columnarBatch.numRows(), source = col)
      }.toList
    )
  }

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  final case class VeColVector(
    numItems: Int,
    veType: VeType,
    containerLocation: Long,
    bufferLocations: List[Long]
  ) {

    /**
     * Sizes of the underlying buffers --- use veType & combination with numItmes to decide them.
     */
    def bufferSizes: List[Int] = veType match {
      case VeScalarType.VeNullableDouble => List(numItems * 8, Math.ceil(numItems / 64.0).toInt * 8)
      case VeString => {
        val dataSize = getUnsafe.getInt(numItems * 4)
        val offsetBuffSize = (numItems + 1) * 4
        val validitySize = Math.ceil(numItems / 64.0).toInt * 8

        List(dataSize, offsetBuffSize, validitySize)
      }
      case _ => ???
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

      val resultingArray = extractBuffers().toArray.flatten

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
    def deserialize(ba: Array[Byte])(implicit veProcess: VeProcess): VeColVector =
      injectBuffers(newBuffers =
        bufferSizes.scanLeft(0)(_ + _).zip(bufferSizes).map { case (bufferStart, bufferSize) =>
          ba.slice(bufferStart, bufferStart + bufferSize)
        }
      ).newContainer()

    private def newContainer()(implicit veProcess: VeProcess): VeColVector = veType match {
      case VeScalarType.VeNullableDouble =>
        val vcvr = new nullable_double_vector()
        vcvr.count = numItems
        vcvr.data = bufferLocations.head
        vcvr.validityBuffer = bufferLocations(1)
        val byteBuffer = nullableDoubleVectorToByteBuffer(vcvr)

        copy(containerLocation = veProcess.putBuffer(byteBuffer))
      case _ => ???
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
      case _ => ???
    }

    def free()(implicit veProcess: VeProcess): Unit =
      (containerLocation :: bufferLocations).foreach(veProcess.free)

  }

  //noinspection ScalaUnusedSymbol
  object VeColVector {

    def fromVectorColumn(numRows: Int, source: ColumnVector)(implicit
      veProcess: VeProcess
    ): VeColVector = {
      source.getArrowValueVector match {
        case float8Vector: Float8Vector   => fromFloat8Vector(float8Vector)
        case bigIntVector: BigIntVector   => fromBigIntVector(bigIntVector)
        case varCharVector: VarCharVector => fromVarcharVector(varCharVector)
        case other                        => sys.error(s"Not supported to convert from ${other.getClass}")
      }
    }

    def fromBigIntVector(bigIntVector: BigIntVector)(implicit veProcess: VeProcess): VeColVector = {
      val vcvr = new nullable_bigint_vector()
      vcvr.count = bigIntVector.getValueCount
      vcvr.data = veProcess.putBuffer(bigIntVector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(bigIntVector.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableBigintVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        numItems = bigIntVector.getValueCount,
        veType = VeScalarType.VeNullableLong,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer)
      )
    }

    def fromFloat8Vector(float8Vector: Float8Vector)(implicit veProcess: VeProcess): VeColVector = {
      val vcvr = new nullable_double_vector()
      vcvr.count = float8Vector.getValueCount
      vcvr.data = veProcess.putBuffer(float8Vector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(float8Vector.getValidityBuffer.nioBuffer())
      val byteBuffer = nullableDoubleVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        numItems = float8Vector.getValueCount,
        veType = VeScalarType.VeNullableDouble,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.validityBuffer)
      )
    }

    def fromVarcharVector(
      varcharVector: VarCharVector
    )(implicit veProcess: VeProcess): VeColVector = {
      val vcvr = new nullable_varchar_vector()
      vcvr.count = varcharVector.getValueCount
      vcvr.data = veProcess.putBuffer(varcharVector.getDataBuffer.nioBuffer())
      vcvr.validityBuffer = veProcess.putBuffer(varcharVector.getValidityBuffer.nioBuffer())
      vcvr.offsets = veProcess.putBuffer(varcharVector.getOffsetBuffer.nioBuffer())
      vcvr.dataSize = varcharVector.sizeOfValueBuffer()
      val byteBuffer = nullableVarCharVectorVectorToByteBuffer(vcvr)
      val containerLocation = veProcess.putBuffer(byteBuffer)
      VeColVector(
        numItems = varcharVector.getValueCount,
        veType = VeString,
        containerLocation = containerLocation,
        bufferLocations = List(vcvr.data, vcvr.offsets, vcvr.validityBuffer)
      )
    }
  }

}
