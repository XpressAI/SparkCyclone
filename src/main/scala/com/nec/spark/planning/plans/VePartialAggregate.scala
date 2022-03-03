package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.concurrent.duration.NANOSECONDS

case class VePartialAggregate(
  expectedOutputs: Seq[NamedExpression],
  partialFunction: VeFunction,
  child: SparkPlan
) extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == partialFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${partialFunction.results.size}"
  )

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time"),
    "inputBatchRows" -> SQLMetrics.createAverageMetric(sparkContext, "input batch row count"),
    "inputBatchCols" -> SQLMetrics.createAverageMetric(sparkContext, "input batch column count"),
    "inputBatchesPerPartition" -> SQLMetrics.createAverageMetric(sparkContext, "input batch count per partition")
  )

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")
    val inputBatchRows = longMetric("inputBatchRows")
    val inputBatchCols = longMetric("inputBatchCols")
    val inputBatchesPerPartition = longMetric("inputBatchesPerPartition")

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        val batches = veColBatches.toList
        inputBatchesPerPartition.set(batches.size)
        withVeLibrary { libRef =>
          logger.info(s"Will map partial aggregates using $partialFunction")
          batches.map { veColBatch =>
            val beforeExec = System.nanoTime()
            inputBatchRows.set(veColBatch.numRows)
            inputBatchCols.set(veColBatch.cols.size)

            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            logger.debug(s"Mapping a VeColBatch $veColBatch")
            VeColBatch.fromList {
              import OriginalCallingContext.Automatic._
              val res =
                try {
                  val result = ImplicitMetrics.processMetrics.measureRunningTime(
                    veProcess.execute(
                      libraryReference = libRef,
                      functionName = partialFunction.functionName,
                      cols = veColBatch.cols,
                      results = partialFunction.namedResults
                    )
                  )(
                    ImplicitMetrics.processMetrics
                      .registerFunctionCallTime(_, veFunction.functionName)
                  )
                  logger.debug(s"Mapped $veColBatch to $result")
                  result
                } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
              execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)

              res
            }
          }
        }.toIterator
      }
  }

  // this is wrong, but just to please spark
  override def output: Seq[Attribute] = expectedOutputs.map(_.toAttribute)

  override def veFunction: VeFunction = partialFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(partialFunction = f(partialFunction))
}
