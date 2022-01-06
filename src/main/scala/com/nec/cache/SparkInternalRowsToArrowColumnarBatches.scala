package com.nec.cache

import com.nec.arrow.ArrowEncodingSettings
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerConversionTime}
object SparkInternalRowsToArrowColumnarBatches {
  def apply(rowIterator: Iterator[InternalRow], arrowSchema: Schema)(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings
  ): Iterator[ColumnarBatch] = {
    if (rowIterator.hasNext) {
      new Iterator[ColumnarBatch] {
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
          measureRunningTime {
            arrowWriter.reset()
            cb.setNumRows(0)
            root.getFieldVectors.asScala.foreach(_.reset())
            var rowCount = 0
            while (rowCount < arrowEncodingSettings.numRows && rowIterator.hasNext) {
              val row = rowIterator.next()
              arrowWriter.write(row)
              arrowWriter.finish()
              rowCount += 1
            }
            cb.setNumRows(rowCount)
            cb
          }(registerConversionTime)
        }
      }
    } else Iterator.empty
  }

}
