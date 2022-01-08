package com.nec.cache

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

object SparkInternalRowsToArrowColumnarBatches {
  def apply(
    rowIterator: Iterator[InternalRow],
    timeZoneId: String,
    schema: StructType,
    numRows: Int
  )(implicit bufferAllocator: BufferAllocator): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      new Iterator[ColumnarBatch] {
        private val arrowSchema = ArrowUtilsExposed.toArrowSchema(schema, timeZoneId)
        private val root = VectorSchemaRoot.create(arrowSchema, bufferAllocator)
        import scala.collection.JavaConverters._
        private val cb = new ColumnarBatch(
          root.getFieldVectors.asScala
            .map(vector => new ArrowColumnVector(vector))
            .toArray,
          root.getRowCount
        )

        private val arrowWriter = ArrowWriter.create(root)
        arrowWriter.finish()
        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          cb.close()
        }

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        override def next(): ColumnarBatch = {
          arrowWriter.reset()
          cb.setNumRows(0)
          root.getFieldVectors.asScala.foreach(_.reset())
          var rowCount = 0
          while (rowCount < numRows && rowIterator.hasNext) {
            val row = rowIterator.next()
            arrowWriter.write(row)
            arrowWriter.finish()
            rowCount += 1
          }
          cb.setNumRows(rowCount)
          cb
        }
      }
    } else Iterator.empty
  }

}
