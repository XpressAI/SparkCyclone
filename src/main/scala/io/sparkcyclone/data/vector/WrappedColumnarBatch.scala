package io.sparkcyclone.data.vector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized._
import org.apache.spark.unsafe.types.UTF8String
import scala.reflect.ClassTag
import java.util.{Iterator => JIterator}
import org.apache.spark.sql.columnar.CachedBatch

/*
  Implementation of a [[ColumnarBatch]] backed by a SparkCyclone-defined
  [[ColBatchLike]] implementation.

  This placeholder class is only intended to be used for returning the output of
  Spark SQL cache deserialization, which requires a [[ColumnarBatch]] to be
  returned.  It is NOT for general consumption, and as such, the `unsupported`
  calls are intentional.
*/
final case class WrappedColumnarBatch[C <: ColVectorLike] private[data] (val underlying: ColBatchLike[C])
  extends ColumnarBatch(Array.empty[ColumnVector], 0) with CachedBatch {

  private[vector] def unsupported: Nothing = {
    throw new UnsupportedOperationException("Operation is not supported - this class is only intended to be a data carrier class.")
  }

  override def close: Unit = {}

  override def rowIterator: JIterator[InternalRow] = unsupported

  override def setNumRows(numRows: Int): Unit = unsupported

  override def numCols: Int = {
    underlying.numCols
  }

  override def numRows: Int = {
    underlying.numRows
  }

  override def sizeInBytes: Long = {
    underlying.sizeInBytes
  }

  override def column(ordinal: Int): ColumnVector = unsupported

  override def getRow(rowId: Int): InternalRow = unsupported
}
