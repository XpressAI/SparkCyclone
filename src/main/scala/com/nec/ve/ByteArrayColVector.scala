package com.nec.ve

import com.nec.spark.agile.SparkExpressionToCExpression.likelySparkType
import com.nec.spark.planning.ArrowSerializedColColumnarVector
import com.nec.ve.VeColBatch.{VeColVectorSource, VectorEngineLocation}
import org.apache.spark.sql.vectorized.ColumnVector

import java.nio.ByteBuffer

object ByteArrayColVector {
  implicit final class RichByteArrayColVector(byteArrayColVector: MaybeByteArrayColVector) {
    import byteArrayColVector._
    def transferBuffersToVe()(implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): ColVector[Option[VectorEngineLocation]] =
      copy(
        buffers = buffers.map { maybeBa =>
          maybeBa.map { ba =>
            /** VE can only take direct byte buffers at the moment */
            val byteBuffer = ByteBuffer.allocateDirect(ba.length)
            byteBuffer.put(ba, 0, ba.length)
            byteBuffer.position(0)
            VectorEngineLocation(veProcess.putBuffer(byteBuffer))
          }
        },
        containerLocation = None,
        source = source
      )
    def transferToByteBuffers(): ByteBufferVeColVector =
      byteArrayColVector.map(_.map { ba =>
        val byteBuffer = ByteBuffer.allocateDirect(ba.length)
        byteBuffer.put(ba, 0, ba.length)
        byteBuffer.position(0)
        byteBuffer
      })

    def toInternalVector: ColumnVector =
      new ArrowSerializedColColumnarVector(byteArrayColVector, likelySparkType(veType))
  }

}
