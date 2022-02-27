package com.nec.spark.planning.plans

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

case class VeFlattenPartition(flattenFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  require(
    output.size == flattenFunction.results.size,
    s"Expected output size ${output.size} to match flatten function results size, but got ${flattenFunction.results.size}"
  )

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time")
  )

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")
    val beforeExec = System.nanoTime()

    val res = child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRefExchange =>
          Iterator
            .continually {
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              logger.info(s"About to fetch VeColBatches for flattening a partition...")
              val inputBatches = veColBatches.toList
              logger.info(s"Fetched all the data: ${inputBatches.toString().take(80)}")
              inputBatches match {
                case one :: Nil => Iterator(one)
                case Nil        => Iterator.empty
                case _ =>
                  import OriginalCallingContext.Automatic._
                  Iterator {
                    VeColBatch.fromList(try {

                      val res =
                        veProcess.executeMultiIn(
                          libraryReference = libRefExchange,
                          functionName = flattenFunction.functionName,
                          batches = VeBatchOfBatches.fromVeColBatches(inputBatches),
                          results = flattenFunction.namedResults
                        )
                      logger.info(s"Transformed input, got ${res}")
                      res
                    } finally {
                      inputBatches
                        .foreach(child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup)
                    })
                  }
              }
            }
            .take(1)
            .flatten
        }
      }
    execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)

    res
  }

  override def output: Seq[Attribute] = child.output

  override def veFunction: VeFunction = flattenFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(flattenFunction = f(flattenFunction))
}
