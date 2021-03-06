package io.sparkcyclone.data.conversion

import io.sparkcyclone.data.VeColVectorSource
import org.apache.spark.sql.catalyst.expressions.{Attribute, PrettyAttribute}
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions._
import io.sparkcyclone.data.vector._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.arrow.vector.types.pojo.Schema

object SparkSqlColumnarBatchConversions {
  implicit class SparkSqlColumnarBatchToBPCV(batch: ColumnarBatch) {
    def columns: Seq[ColumnVector] = {
      (0 until batch.numCols).map(batch.column(_))
    }

    def attributes: Seq[Attribute] = {
      batch.columns.zipWithIndex.map {
        case (col, i) => PrettyAttribute(s"_${i}", col.dataType)
      }
    }

    def toBytePointerColBatch(schema: Schema)(implicit source: VeColVectorSource): BytePointerColBatch = {
      batch match {
        case WrappedColumnarBatch(wrapped: BytePointerColBatch) =>
          wrapped

        case WrappedColumnarBatch(wrapped: ByteArrayColBatch) =>
          wrapped.toBytePointerColBatch

        case WrappedColumnarBatch(wrapped) =>
          throw new IllegalArgumentException(s"Cannot convert WrappedColumnarBatch[${wrapped.getClass.getSimpleName}] to BytePointerColBatch")

        case _ =>
          val bpcolumns = columns.zipWithIndex.map { case (column, i) =>
            column.extractArrowVector match {
              case Some(arrowvec) =>
                arrowvec.toBytePointerColVector

              case None =>
                val field = schema.getFields.get(i)
                column.toBytePointerColVector(field.getName, batch.numRows)
            }
          }

          BytePointerColBatch(bpcolumns)
      }
   }
  }
}