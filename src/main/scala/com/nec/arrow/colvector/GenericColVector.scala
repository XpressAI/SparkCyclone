package com.nec.arrow.colvector

import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString, VeType}
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

  def toUnit: UnitColVector = UnitColVector(map(_ => ()))

  if (veType == VeString) require(variableSize.nonEmpty, "String should come with variable size")

  def containerLocation: Data = container

  def nonEmpty: Boolean = numItems > 0
  def isEmpty: Boolean = !nonEmpty

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

  def map[Other](f: Data => Other): GenericColVector[Other] =
    copy(container = f(containerLocation), buffers = buffers.map(f))

}

object GenericColVector {
  def bufCount(veType: VeType): Int = veType match {
    case VeString => 3
    case _ => 2
  }
}
