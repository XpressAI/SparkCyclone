package com.nec.spark.planning.aggregation

import com.nec.spark.SparkCycloneExecutorPlugin.{cycloneMetrics, source}
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsKeyedVeColBatch, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeRDD.RichKeyedRDDL
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.concurrent.duration.NANOSECONDS

case class VeHashExchangePlan(exchangeFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction
  with SupportsKeyedVeColBatch {

  import OriginalCallingContext.Automatic._
  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time")
  )
  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        val beforeExec = System.nanoTime()

        val res = withVeLibrary { libRefExchange =>
          logger.info(s"Will map multiple col batches for hash exchange.")
          veColBatches.flatMap { veColBatch =>
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            try {
              logger.debug(s"Mapping ${veColBatch} for exchange")
              val multiBatches = cycloneMetrics.measureRunningTime(
                veProcess.executeMulti(
                  libraryReference = libRefExchange,
                  functionName = exchangeFunction.functionName,
                  cols = veColBatch.cols,
                  results = exchangeFunction.namedResults
                )
              )(cycloneMetrics.registerFunctionCallTime(_, veFunction.functionName))
              logger.debug(s"Mapped to ${multiBatches} completed.")

              multiBatches.flatMap {
                case (n, l) if l.head.nonEmpty =>
                  Option(n -> VeColBatch.fromList(l))
                case (_, l) =>
                  l.foreach(_.free())
                  None
              }
            } finally {
              child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
            }
          }
        }
        execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)
        res
      }
      .exchangeBetweenVEs(cleanUpInput = true)


  }

  override def output: Seq[Attribute] = child.output

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(exchangeFunction = f(exchangeFunction))

  override def veFunction: VeFunction = exchangeFunction

  override def executeVeColumnarKeyed(): RDD[(Int, VeColBatch)] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      withVeLibrary { libRefExchange =>
        logger.info(s"Will map multiple col batches for hash exchange.")
        veColBatches.flatMap { veColBatch =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          try {
            if (veColBatch.nonEmpty) {
              logger.debug(s"Mapping ${veColBatch} for exchange")
              val multiBatches = veProcess.executeMulti(
                libraryReference = libRefExchange,
                functionName = exchangeFunction.functionName,
                cols = veColBatch.cols,
                results = exchangeFunction.namedResults
              )
              logger.debug(s"Mapped to ${multiBatches} completed.")

              multiBatches.flatMap {
                case (n, l) if l.head.nonEmpty =>
                  Option(n -> VeColBatch.fromList(l))
                case (_, l) =>
                  l.foreach(_.free())
                  None
              }
            } else {
              logger.debug(s"${veColBatch} was empty.")
              Nil
            }
          } finally {
            child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
          }
        }
      }
    }

}
