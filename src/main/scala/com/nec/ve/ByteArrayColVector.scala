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
            VectorEngineLocation(veProcess.putBuffer(ByteBuffer.wrap(ba)))
          }
        },
        containerLocation = None,
        source = source
      )
    def transferToByteBuffers(): ByteBufferVeColVector =
      byteArrayColVector.map(_.map { ba =>
        ByteBuffer.wrap(ba)
      })

    def toInternalVector: ColumnVector =
      new ArrowSerializedColColumnarVector(byteArrayColVector, likelySparkType(veType))
  }

}
