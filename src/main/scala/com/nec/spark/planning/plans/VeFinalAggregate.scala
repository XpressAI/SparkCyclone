package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerFunctionCallTime}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.concurrent.duration.NANOSECONDS

case class VeFinalAggregate(
  expectedOutputs: Seq[NamedExpression],
  finalFunction: VeFunction,
  child: SparkPlan
) extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == finalFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${finalFunction.results.size}"
  )

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time")
  )

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")
    val beforeExec = System.nanoTime()

    val res = child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRef =>
          veColBatches.map { veColBatch =>
            logger.debug(s"Preparing to final-aggregate a batch... ${veColBatch}")

            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            VeColBatch.fromList {
              import OriginalCallingContext.Automatic._

              try measureRunningTime(
                veProcess.execute(
                  libraryReference = libRef,
                  functionName = finalFunction.functionName,
                  cols = veColBatch.cols,
                  results = finalFunction.namedResults
                )
              )(registerFunctionCallTime(_, veFunction.functionName))
              finally {
                logger.debug("Completed a final-aggregate of  a batch...")
                child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
              }
            }
          }
        }
      }
    execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)

    res
  }

  override def output: Seq[Attribute] = expectedOutputs.map(_.toAttribute)

  override def veFunction: VeFunction = finalFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(finalFunction = f(finalFunction))
}
