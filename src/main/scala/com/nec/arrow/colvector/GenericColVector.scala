package com.nec.arrow.colvector

import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}
import com.nec.ve.VeColBatch.VeColVectorSource

final case class GenericColVector[Data](
  source: VeColVectorSource,
  numItems: Int,
  name: String,
  variableSize: Option[Int],
  veType: VeType,
  container: Data,
  buffers: List[Data]
) {

  require(
    bufferSizes.size == buffers.size,
    s"Expecting buffersizes to equal buffers in size (${bufferSizes.size} & ${buffers.size})"
  )

  require(
    bufferSizes.size == buffers.size,
    s"Expecting buffersizes to equal buffers in size (${bufferSizes.size} & ${buffers.size})"
  )

  def containerLocation: Data = container

  def nonEmpty: Boolean = numItems > 0
  def isEmpty: Boolean = !nonEmpty

  if (veType == VeString) require(variableSize.nonEmpty, "String should come with variable size")

  /**
   * Sizes of the underlying buffers --- use veType & combination with numItmes to decide them.
   */
  def bufferSizes: List[Int] = veType match {
    case v: VeScalarType =>
      List(
        numItems * v.cSize,
        Math.ceil(numItems / 64.0).toInt * 8
      )

    case VeString =>
      val offsetBuffSize = numItems * 4
      val lenghtsSize = numItems * 4
      val validitySize = Math.ceil(numItems / 64.0).toInt * 8

      variableSize.toList.map(_ * 4) ++ List(
        offsetBuffSize,
        lenghtsSize,
        validitySize
      )
  }

  def containerSize: Int = veType.containerSize

  def map[Other](f: Data => Other): GenericColVector[Other] =
    copy(container = f(containerLocation), buffers = buffers.map(f))

}

object GenericColVector {
  def bufCount(veType: VeType): Int = veType match {
    case VeString => 4
    case _        => 2
  }
}
