package com.nec.ve

import com.nec.arrow.ArrowTransferStructures.{non_null_c_bounded_string, nullable_double_vector, nullable_varchar_vector}
import com.nec.arrow.VeArrowTransfers.{nullableDoubleVectorToByteBuffer, nullableVarCharVectorVectorToByteBuffer}
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.spark.agile.SparkExpressionToCExpression.sparkTypeToVeType
import com.nec.arrow.ArrowTransferStructures.nullable_double_vector
import com.nec.arrow.VeArrowTransfers.nullableDoubleVectorToByteBuffer
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeType}
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, Float8Vector, VarCharVector}

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
    def serialize(): Array[Byte] = ???

    def deserialize(ba: Array[Byte])(implicit veProcess: VeProcess): VeColVector = ???

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
      // todo support more than Arrow
      fromFloat8Vector(source.getArrowValueVector.asInstanceOf[Float8Vector])
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

    def fromVarcharVector(varcharVector: VarCharVector)(implicit veProcess: VeProcess): VeColVector = {
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
