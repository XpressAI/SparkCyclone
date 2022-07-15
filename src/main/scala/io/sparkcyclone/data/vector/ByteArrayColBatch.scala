package io.sparkcyclone.data.vector

import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.vectorized.ColumnarBatch

final case class ByteArrayColBatch(columns: Seq[ByteArrayColVector]) extends CachedBatch {
  if (columns.nonEmpty) {
    require(
      columns.map(_.numItems).toSet.size == 1,
      s"Columns of ${getClass.getSimpleName} should have the same number of rows"
    )
  }

  def numRows: Int = {
    columns.headOption.map(_.numItems).getOrElse(0)
  }

  def sizeInBytes: Long = {
    columns.flatMap(_.buffers).map(_.size.toLong).foldLeft(0L)(_ + _)
  }

  def toSparkColumnarBatch: ColumnarBatch = {
    val batch = new ColumnarBatch(columns.map(_.toSparkColumnVector).toArray)
    batch.setNumRows(numRows)
    batch
  }
}
