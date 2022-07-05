package io.sparkcyclone.colvector

import org.apache.spark.sql.vectorized.ColumnarBatch

final case class ByteArrayColBatch(columns: Seq[ByteArrayColVector]) {
  if (columns.nonEmpty) {
    require(
      columns.map(_.numItems).toSet.size == 1,
      s"Columns of ${getClass.getSimpleName} should have the same number of rows"
    )
  }

  def numRows: Int = {
    columns.headOption.map(_.numItems).getOrElse(0)
  }

  def toSparkColumnarBatch: ColumnarBatch = {
    val batch = new ColumnarBatch(columns.map(_.toSparkColumnVector).toArray)
    batch.setNumRows(numRows)
    batch
  }
}
