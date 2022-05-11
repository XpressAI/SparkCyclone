package com.nec.colvector

import com.nec.spark.agile.core.VeType

final case class ColumnGroup(columns: Seq[VeColVector]) {
  require(
    columns.nonEmpty,
    s"[${getClass.getName}] Need at least one column!"
  )

  require(
    columns.map(_.veType).toSet.size == 1,
    s"[${getClass.getName}] VeTypes must match!"
  )

  def veType: VeType = {
    columns.head.veType
  }
}

final case class VeBatchOfBatches(batches: Seq[VeColBatch]) {
  def veTypes: Seq[VeType] = {
    batches.headOption.toSeq.flatMap(_.veTypes)
  }

  def numColumns: Int = {
    batches.head.columns.size
  }

  def numRows: Int = {
    batches.map(_.numRows).fold(0)(_ + _)
  }

  def isEmpty: Boolean = {
    !nonEmpty
  }

  def nonEmpty: Boolean = {
    numRows > 0
  }

  /*
    Transpose to get the columns from each batch aligned, e.g.

      B1: [ V1, V2, V3 ]
      B2: [ W1, W2, W3 ]

      =>

      [[ V1, W1 ], [ V2, W2 ], [ V3, W3 ]]
  */
  def groupedColumns: Seq[ColumnGroup] = {
    batches.map(_.columns).transpose.map(ColumnGroup(_))
  }
}
