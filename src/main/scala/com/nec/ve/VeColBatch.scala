package com.nec.ve

import com.nec.arrow.ArrowTransferStructures.nullable_double_vector
import com.nec.arrow.VeArrowNativeInterface.copyBufferToVe
import com.nec.arrow.VeArrowTransfers.nullableDoubleVectorToByteBuffer
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeType}
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, Float8Vector}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
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
    ???
  }

  final case class VeColVector(
    numItems: Long,
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
        val structVector = new nullable_double_vector()
        val byteBuffer = nullableDoubleVectorToByteBuffer(structVector)
        val veoPtr = byteBuffer.getLong(0)
        val validityPtr = byteBuffer.getLong(8)
        val dataCount = byteBuffer.getInt(16)
        if (dataCount > 0) {
          val dataSize = dataCount * 8
          val vhTarget = ByteBuffer.allocateDirect(dataSize)
          val validityTarget = ByteBuffer.allocateDirect(dataCount)
          veProcess.get(veoPtr, vhTarget, vhTarget.limit())
          veProcess.get(validityPtr, validityTarget, validityTarget.limit())
        }
        float8Vector
      case _ => ???
    }

    def free()(implicit veProcess: VeProcess): Unit =
      (containerLocation :: bufferLocations).foreach(veProcess.free)

  }

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
