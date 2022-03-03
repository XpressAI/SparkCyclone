package com.nec.spark.planning.plans

import com.nec.arrow.ArrowEncodingSettings
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeBatchOfBatches
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.concurrent.duration.NANOSECONDS

case class VeAmplifyBatchesPlan(amplifyFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  require(
    output.size == amplifyFunction.results.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${amplifyFunction.results.size}"
  )

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time"),
    "batchBufferSize" -> SQLMetrics.createSizeMetric(sparkContext, "batch buffer size"),
    "inputBatchCount" -> SQLMetrics.createMetric(sparkContext, "input batch count"),
    "inputRowCount" -> SQLMetrics.createMetric(sparkContext, "input row count"),
    "inputColCount" -> SQLMetrics.createMetric(sparkContext, "input col count"),
    "outputBatchCount" -> SQLMetrics.createMetric(sparkContext, "output batch count")
  )

  private val encodingSettings = ArrowEncodingSettings.fromConf(conf)(sparkContext)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")
    val inputBatchCount = longMetric("inputBatchCount")
    val inputRowCount = longMetric("inputRowCount")
    val inputColCount = longMetric("inputColCount")
    val outputBatchCount = longMetric("outputBatchCount")
    val batchBufferSize = longMetric("batchBufferSize")

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        val batches = veColBatches.toList
        inputBatchCount.set(batches.size)
        batches.foreach {b =>
          batchBufferSize.add(b.totalBufferSize)
          inputRowCount.set(b.numRows)
          inputColCount.set(b.cols.size)
        }

        val res = withVeLibrary { libRefExchange =>
          import com.nec.util.BatchAmplifier.Implicits._
          batches.iterator
            .amplify(limit = encodingSettings.batchSizeTargetBytes, f = _.totalBufferSize)
            .map {
              case inputBatches if inputBatches.size == 1 => {
                outputBatchCount.set(1)
                inputBatches.head
              }
              case inputBatches =>
                import OriginalCallingContext.Automatic._

                val beforeExec = System.nanoTime()

                val res = try {
                  val res =
                    VeColBatch.fromList(
                      veProcess.executeMultiIn(
                        libraryReference = libRefExchange,
                        functionName = amplifyFunction.functionName,
                        batches = VeBatchOfBatches.fromVeColBatches(inputBatches.toList),
                        results = amplifyFunction.namedResults
                      )
                    )
                  logger.debug(
                    s"Transformed input, got ${res}; produced a batch of size ${res.totalBufferSize} bytes"
                  )
                  res
                } finally {
                  inputBatches
                    .foreach(child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup)
                }

                outputBatchCount.set(1)
                execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)
                res
            }
        }
        res
      }
  }

  override def output: Seq[Attribute] = child.output

  override def veFunction: VeFunction = amplifyFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(amplifyFunction = f(amplifyFunction))
}
