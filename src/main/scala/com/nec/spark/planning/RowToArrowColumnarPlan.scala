package com.nec.spark.planning

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.collection.JavaConverters.asScalaBufferConverter

case class RowToArrowColumnarPlan(child: SparkPlan) extends RowToColumnarTransition {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches")
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows: Int = sparkContext.getConf
      .getOption("com.nec.spark.ve.columnBatchSize")
      .map(_.toInt)
      .getOrElse(conf.columnBatchSize)
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    child.execute().mapPartitions { rowIterator =>
      if (rowIterator.hasNext) {
        lazy implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"Writer for partial collector (Arrow)", 0, Long.MaxValue)
        new Iterator[ColumnarBatch] {
          private val timeZoneId = conf.sessionLocalTimeZone

          private val arrowSchema = ArrowUtilsExposed.toArrowSchema(child.schema, timeZoneId)
          private val root = VectorSchemaRoot.create(arrowSchema, allocator)
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
            cb.setNumRows(0)
            root.getFieldVectors.asScala.foreach(_.reset())
            var rowCount = 0
            while (rowCount < numRows && rowIterator.hasNext) {
              val row = rowIterator.next()
              arrowWriter.write(row)
              rowCount += 1
            }
            cb.setNumRows(rowCount)
            numInputRows += rowCount
            numOutputBatches += 1
            cb
          }
        }
      } else {
        Iterator.empty
      }
    }
  }
}
