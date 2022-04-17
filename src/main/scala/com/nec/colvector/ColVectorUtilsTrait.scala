package com.nec.colvector

import com.nec.spark.agile.core.{VeScalarType, VeString, VeType}

trait ColVectorUtilsTrait {
  def numItems: Int
  def veType: VeType
  def dataSize: Option[Int]

  final def nonEmpty: Boolean = {
    numItems > 0
  }

  final def isEmpty: Boolean = {
    !nonEmpty
  }

  final def bufferSizes: Seq[Int] = {
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
