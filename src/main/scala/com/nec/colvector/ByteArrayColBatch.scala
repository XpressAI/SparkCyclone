package com.nec.colvector

import org.apache.spark.sql.vectorized.ColumnarBatch

final case class ByteArrayColBatch(columns: Seq[ByteArrayColVector]) {
  def numRows: Int = {
    columns.head.numItems
  }

  def toSparkColumnarBatch: ColumnarBatch = {
    val batch = new ColumnarBatch(columns.map(_.toSparkColumnVector).toArray)
    batch.setNumRows(numRows)
    batch
  }
}
