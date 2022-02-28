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
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time")
  )

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRef =>
          logger.info(s"Will map partial aggregates using $partialFunction")
          veColBatches.map { veColBatch =>
            val beforeExec = System.nanoTime()

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
        }
      }
  }

  // this is wrong, but just to please spark
  override def output: Seq[Attribute] = expectedOutputs.map(_.toAttribute)

  override def veFunction: VeFunction = partialFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(partialFunction = f(partialFunction))
}
