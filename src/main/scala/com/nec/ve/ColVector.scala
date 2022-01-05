package com.nec.ve

import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
import com.nec.ve.VeColBatch.VeColVectorSource

final case class ColVector[Data](
  source: VeColVectorSource,
  numItems: Int,
  name: String,
  variableSize: Option[Int],
  veType: VeType,
  containerLocation: Data,
  buffers: List[Data]
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

  def map[Other](f: Data => Other): ColVector[Other] =
    copy(containerLocation = f(containerLocation), buffers = buffers.map(f))

}
