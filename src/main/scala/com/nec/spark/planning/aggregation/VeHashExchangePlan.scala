package com.nec.spark.planning.aggregation

import com.nec.colvector.VeColBatch
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import com.nec.util.CallContextOps._
import com.nec.spark.planning._
import com.nec.util.CallContext
import com.nec.ve.VeRDDOps.RichKeyedRDDL
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeHashExchangePlan(exchangeFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction
  with PlanMetrics
  with SupportsKeyedVeColBatch {


  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

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
              import com.nec.spark.SparkCycloneExecutorPlugin.{veProcess, veMetrics}
              try {
                logger.debug(s"Mapping ${veColBatch} for exchange")
                val multiBatches = withInvocationMetrics(VE){
                  vectorEngine.executeMulti(
                    libRefExchange,
                    exchangeFunction.functionName,
                    veColBatch.columns.toList,
                    exchangeFunction.namedResults
                  )
                }
                logger.debug(s"Mapped to ${multiBatches} completed.")

                multiBatches.flatMap {
                  case (n, l) if l.head.nonEmpty =>
                    Option(n -> VeColBatch(l))
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
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRefExchange =>
          logger.info(s"Will map multiple col batches for hash exchange.")
          incrementInvocations(PLAN)
          collectBatchMetrics(OUTPUT, veColBatches.flatMap { veColBatch =>
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            collectBatchMetrics(INPUT, veColBatch)
            withInvocationMetrics(BATCH){
              try {
                if (veColBatch.nonEmpty) {
                  logger.debug(s"Mapping ${veColBatch} for exchange")
                  val multiBatches = withInvocationMetrics(VE){
                    vectorEngine.executeMulti(
                      libRefExchange,
                      exchangeFunction.functionName,
                      veColBatch.columns.toList,
                      exchangeFunction.namedResults
                    )
                  }
                  logger.debug(s"Mapped to ${multiBatches} completed.")

                  multiBatches.flatMap {
                    case (n, l) if l.head.nonEmpty =>
                      Option(n -> VeColBatch(l))
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
          })
        }
      }
  }

}
