package com.nec.colvector

import com.nec.colvector.SeqOptTConversions._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.unsafe.types.UTF8String
import org.bytedeco.javacpp.BytePointer

final case class BytePointerColBatch(columns: Seq[BytePointerColVector]) {
  private[colvector] lazy val projection = {
    UnsafeProjection.create(sparkSchema.toArray)
  }

  private[colvector] lazy val dcolumns = {
    columns.map(_.toSeqOptAny)
  }

  def sparkSchema: Seq[DataType] = {
    columns.map(_.veType.toSparkType)
  }

  def internalRowIterator: Iterator[InternalRow] = {
    /*
      Construct `UnsafeRow` from `InternalRow`, since Spark complains about not
      being able to cast from `InternalRow` to `UnsafeRow`:

        https://github.com/apache/spark/blob/master/sql/core/src/test/scala/org/apache/spark/sql/UnsafeRowSuite.scala
    */

    new Iterator[InternalRow] {
      private val iterators = dcolumns.map(_.iterator)

      override def hasNext: Boolean = {
        iterators.headOption.map(_.hasNext).getOrElse(false)
      }

      override def next: InternalRow = {
        projection.apply(InternalRow(iterators.map(_.next).map(_.getOrElse(null)): _*))
      }
    }
  }
}
