package com.nec.ve

import com.nec.arrow.ArrowInterfaces.nullable_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.nullable_double_vector
import com.nec.arrow.VeArrowNativeInterface.copyBufferToVe
import com.nec.arrow.VeArrowTransfers.nullableDoubleVectorToByteBuffer
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeType}
import com.nec.spark.agile.SparkExpressionToCExpression.sparkTypeToVeType
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, Float8Vector}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
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
        val veType = sparkTypeToVeType(sparkType)
        VeColVector(
          numItems = columnarBatch.numRows(),
          veType = veType,
          containerLocation = ???,
          containerSize = ???,
          bufferLocations = ???
        )
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
    containerSize: Int,
    bufferLocations: List[Long]
  ) {

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
      case _ => ???
    }

    def free()(implicit veProcess: VeProcess): Unit =
      (containerLocation :: bufferLocations).foreach(veProcess.free)

  }

  //noinspection ScalaUnusedSymbol
  object VeColVector {
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
        containerSize = byteBuffer.limit(),
        bufferLocations = List(vcvr.data, vcvr.validityBuffer)
      )
    }
  }

}