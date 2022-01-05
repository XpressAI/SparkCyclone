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
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource, VectorEngineLocation}

import java.nio.ByteBuffer

final case class ColVector[Data](
  source: VeColVectorSource,
  numItems: Int,
  name: String,
  variableSize: Option[Int],
  veType: VeType,
  containerLocation: Data,
  bufferLocations: List[Data]
) {

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

  def containerSize: Int = veType.containerSize

  def map[Other](f: Data => Other): ColVector[Other] = copy(
    containerLocation = f(containerLocation),
    bufferLocations = bufferLocations.map(f)
  )

}

object ColVector {
  type MaybeByteArrayColVector = ColVector[Option[Array[Byte]]]
  implicit final class RichByteArrayColVector(byteArrayColVector: MaybeByteArrayColVector) {
    import byteArrayColVector._
    def transferBuffersToVe()(implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): ColVector[Option[VectorEngineLocation]] =
      copy(
        bufferLocations = bufferLocations.map { maybeBa =>
          maybeBa.map { ba =>
            val byteBuffer = ByteBuffer.allocateDirect(ba.length)
            byteBuffer.put(ba, 0, ba.length)
            byteBuffer.position(0)
            VectorEngineLocation(veProcess.putBuffer(byteBuffer))
          }
        },
        containerLocation = None,
        source = source
      )
  }

}
