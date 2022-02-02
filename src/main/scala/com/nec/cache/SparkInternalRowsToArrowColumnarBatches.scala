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
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging

object SparkInternalRowsToArrowColumnarBatches extends LazyLogging {
  def apply(
    rowIterator: Iterator[InternalRow],
    arrowSchema: Schema,
    completeInSpark: Boolean = true
  )(implicit
    bufferAllocator: BufferAllocator,
    arrowEncodingSettings: ArrowEncodingSettings,
    originalCallingContext: OriginalCallingContext
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

        private lazy val fv = root.getFieldVectors.asScala.flatMap(_.getFieldBuffers.asScala).toList

        def totalVectorSize: Long = fv.foldLeft(0L) { case (s, ab) => s + ab.capacity() }

        private val arrowWriter = ArrowWriter.create(root)
        arrowWriter.finish()

        override def hasNext: Boolean = {
          rowIterator.hasNext
        }

        if (completeInSpark) {
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            cb.close()
          }
        }

        override def next(): ColumnarBatch = {
          measureRunningTime {
            arrowWriter.reset()
            cb.setNumRows(0)
            root.getFieldVectors.asScala.foreach(_.reset())
            var rowCount = 0

            /** We try to fill up as much as possible either if the row count is too small or if the total size is too small */
            def tryToAddMore: Boolean =
              rowCount < arrowEncodingSettings.numRows || totalVectorSize < arrowEncodingSettings.batchSizeTargetBytes

            while (tryToAddMore && rowIterator.hasNext) {
              val row = rowIterator.next()
              arrowWriter.write(row)
              arrowWriter.finish()
              rowCount += 1
            }
            cb.setNumRows(rowCount)
            logger.debug(
              s"Emitted batch of size ${totalVectorSize} bytes (${rowCount} rows); requested from ${originalCallingContext.renderString}."
            )
            cb
          }(registerConversionTime)
        }
      }
    } else Iterator.empty
  }

}
