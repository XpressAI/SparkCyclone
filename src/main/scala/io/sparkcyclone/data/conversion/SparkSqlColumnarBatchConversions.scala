package io.sparkcyclone.data.conversion

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions._
import io.sparkcyclone.data.vector.{BytePointerColVector, BytePointerColBatch}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.arrow.vector.types.pojo.Schema

object SparkSqlColumnarBatchConversions {
  implicit class SparkSqlColumnarBatchToBPCV(batch: ColumnarBatch) {
    def columns: Seq[ColumnVector] = {
      (0 until batch.numCols).map(batch.column(_))
    }

    def toBytePointerColBatch(schema: Schema)(implicit source: VeColVectorSource): BytePointerColBatch = {
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