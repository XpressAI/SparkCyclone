package io.sparkcyclone.data.vector

import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.vectorized.ColumnarBatch

final case class ByteArrayColBatch private[data] (columns: Seq[ByteArrayColVector]) extends ColBatchLike[ByteArrayColVector] {
  if (columns.nonEmpty) {
    require(
      columns.map(_.numItems).toSet.size == 1,
      s"Columns of ${getClass.getSimpleName} should have the same number of rows"
    )
  }

  override def sizeInBytes: Long = {
    columns.flatMap(_.buffers).map(_.size.toLong).foldLeft(0L)(_ + _)
  }

  def toBytePointerColBatch: BytePointerColBatch = {
    BytePointerColBatch(columns.map(_.toBytePointerColVector))
  }
}
