package com.nec.spark.planning

import com.nec.ve.VeColBatch
import org.apache.spark.SparkContext

import scala.language.implicitConversions
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable
import scala.concurrent.duration.NANOSECONDS

trait PlanMetrics {
  def BATCH = "batch"
  def PLAN = "plan"
  def VE = "VE"

  def INPUT = "input"
  def OUTPUT = "output"

  protected def sparkContext: SparkContext
  def longMetric(name: String): SQLMetric


  def partitionMetrics(metricPrefix: String): mutable.HashMap[String,SQLMetric] = {
    val ret = mutable.HashMap[String,SQLMetric]()
    for(i <- 0 until 10) {
      ret(s"$i{metricPrefix}inPartitions") =  SQLMetrics.createMetric(sparkContext,s"${metricPrefix} partitions")
    }
    ret
  }
  def invocationMetrics(metricPrefix: String) = Map(
    s"${metricPrefix}Exec" -> SQLMetrics.createTimingMetric(sparkContext, s"${metricPrefix} execution time"),
    s"${metricPrefix}Invocations" -> SQLMetrics.createMetric(sparkContext, s"${metricPrefix} invocation count")
  )

  def batchMetrics(metricPrefix: String) = Map(
    s"${metricPrefix}TotalBatchRowCount" -> SQLMetrics.createMetric(sparkContext, s"total ${metricPrefix} batch row count"),
    s"${metricPrefix}AvgBatchColCount" -> SQLMetrics.createAverageMetric(sparkContext, s"${metricPrefix} batch column count"),
    s"${metricPrefix}AvgBatchRowCount" -> SQLMetrics.createAverageMetric(sparkContext, s"${metricPrefix} batch row count"),
    s"${metricPrefix}BatchSize" -> SQLMetrics.createSizeMetric(sparkContext, s"${metricPrefix} batch size"),
    s"${metricPrefix}BatchCount" -> SQLMetrics.createMetric(sparkContext, s"${metricPrefix} batch count")
  )

  def incrementInvocations(metricPrefix: String): Unit = {
    longMetric(s"${metricPrefix}Invocations").add(1)
  }


  def collectPartitionMetrics[T](metricPrefix: String,numPartitions: Long) {
    val execMetric = longMetric(s"${metricPrefix}inPartitions")
    execMetric.set(numPartitions)
  }

  def withInvocationMetrics[T](metricPrefix: String)(f: => T): T = {
    incrementInvocations(metricPrefix)
    val execMetric = longMetric(s"${metricPrefix}Exec")
    val beforeExec = System.nanoTime()
    val res = f
    execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)
    res
  }

  def incrementBatchCount(metricPrefix: String): Unit = {
    longMetric(s"${metricPrefix}BatchCount").add(1)
  }

  def collectBatchMetrics(metricPrefix: String, batch: ColumnarBatch): ColumnarBatch = {
    incrementBatchCount(metricPrefix)

    val batchColCount = longMetric(s"${metricPrefix}AvgBatchColCount")
    val batchRowCount = longMetric(s"${metricPrefix}AvgBatchRowCount")
    val totalBatchRowCount = longMetric(s"${metricPrefix}TotalBatchRowCount")

    batchColCount.set(batch.numCols())
    batchRowCount.set(batch.numRows())
    totalBatchRowCount += batch.numRows()
    batch
  }

  def collectBatchMetrics(metricPrefix: String, batch: VeColBatch): VeColBatch = {
    incrementBatchCount(metricPrefix)

    val batchColCount = longMetric(s"${metricPrefix}AvgBatchColCount")
    val batchRowCount = longMetric(s"${metricPrefix}AvgBatchRowCount")
    val totalBatchRowCount = longMetric(s"${metricPrefix}TotalBatchRowCount")
    val batchSize = longMetric(s"${metricPrefix}BatchSize")

    batchColCount.set(batch.cols.length)
    batchRowCount.set(batch.numRows)
    batchSize.set(batch.totalBufferSize)

    totalBatchRowCount += batch.numRows

    batch
  }

  def collectBatchMetrics[T](metricPrefix: String, batches: Iterator[T]): Iterator[T] = {
    batches.map {
      case b: VeColBatch => collectBatchMetrics(metricPrefix, b).asInstanceOf[T]
      case (k: Any, b: VeColBatch) => (k, collectBatchMetrics(metricPrefix, b)).asInstanceOf[T]
      case b: ColumnarBatch => collectBatchMetrics(metricPrefix, b).asInstanceOf[T]
      case (k: Any, b: ColumnarBatch) => (k, collectBatchMetrics(metricPrefix, b)).asInstanceOf[T]
    }
  }
}
