package io.sparkcyclone.data.vector

import io.sparkcyclone.native.code.{VeScalarType, VeString, VeType}

trait ColVectorLike {
  def name: String
  def veType: VeType
  def numItems: Int
  def dataSize: Option[Int]

  final def nonEmpty: Boolean = {
    numItems > 0
  }

  final def isEmpty: Boolean = {
    !nonEmpty
  }

  final def bufferSizes: Seq[Int] = {
    /*
      NOTE: Arrow relies on the validity buffer to be aligned on 8-byte boundaries
      to work correctly:

      https://wesm.github.io/arrow-site-test/format/Layout.html#alignment-and-padding
    */
    val validitySize = Math.ceil(numItems / 64.0).toInt * 8

    veType match {
      case stype: VeScalarType =>
        Seq(
          numItems * stype.cSize,
          validitySize
        )

      case VeString =>
        dataSize.toSeq.map(_ * 4) ++
        Seq(
          numItems * 4,
          numItems * 4,
          validitySize
        )
    }
  }
}
