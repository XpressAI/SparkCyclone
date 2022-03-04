package com.nec.spark.planning.aggregation

import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, source}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsKeyedVeColBatch, SupportsVeColBatch, VeFunction}
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
  with PlanMetrics
  with SupportsKeyedVeColBatch {

  import OriginalCallingContext.Automatic._
  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        incrementInvocations(PLAN)

        withVeLibrary { libRefExchange =>
          logger.info(s"Will map multiple col batches for hash exchange.")
          collectBatchMetrics(OUTPUT, veColBatches.flatMap { veColBatch =>
            collectBatchMetrics(INPUT, veColBatch)
            withInvocationMetrics(BATCH){
              import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
              try {
                logger.debug(s"Mapping ${veColBatch} for exchange")
                val multiBatches = withInvocationMetrics(VE){
                  ImplicitMetrics.processMetrics.measureRunningTime(
                    veProcess.executeMulti(
                      libraryReference = libRefExchange,
                      functionName = exchangeFunction.functionName,
                      cols = veColBatch.cols,
                      results = exchangeFunction.namedResults
                    )
                  )(ImplicitMetrics.processMetrics.registerFunctionCallTime(_, veFunction.functionName))
                }
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
          })
        }
      }
      .exchangeBetweenVEs(cleanUpInput = true)
  }

  override def output: Seq[Attribute] = child.output

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(exchangeFunction = f(exchangeFunction))

  override def veFunction: VeFunction = exchangeFunction

  override def executeVeColumnarKeyed(): RDD[(Int, VeColBatch)] = {
    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRefExchange =>
          logger.info(s"Will map multiple col batches for hash exchange.")
          incrementInvocations(PLAN)
          veColBatches.flatMap { veColBatch =>
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            withInvocationMetrics(BATCH){
              try {
                if (veColBatch.nonEmpty) {
                  logger.debug(s"Mapping ${veColBatch} for exchange")
                  val multiBatches = withInvocationMetrics(VE){
                    veProcess.executeMulti(
                      libraryReference = libRefExchange,
                      functionName = exchangeFunction.functionName,
                      cols = veColBatch.cols,
                      results = exchangeFunction.namedResults
                    )
                  }
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
  }

}
