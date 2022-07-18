package io.sparkcyclone.data.vector

import io.sparkcyclone.native.code.VeType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, PrettyAttribute}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColBatchLike[+C <: ColVectorLike] extends SimpleMetricsCachedBatch {
  def columns: Seq[C]

  final def numCols: Int = {
    columns.size
  }

  final def numRows: Int = {
    columns.headOption.map(_.numItems).getOrElse(0)
  }

  final def isEmpty: Boolean = {
    numRows <= 0
  }

  final def nonEmpty: Boolean = {
    ! isEmpty
  }

  final def veTypes: Seq[VeType] = {
    columns.map(_.veType)
  }

  final def sparkSchema: Seq[DataType] = {
    veTypes.map(_.toSparkType)
  }

  final def sparkAttributes: Seq[Attribute] = {
    columns.map { col => PrettyAttribute(col.name, col.veType.toSparkType) }
  }

  final def toSparkColumnarBatch: ColumnarBatch = {
    WrappedColumnarBatch(this)
  }

  final val stats: InternalRow = {
    InternalRow.fromSeq(Array[Any](null, null, 0L, numRows, sizeInBytes))
  }
}
