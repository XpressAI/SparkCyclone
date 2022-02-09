package com.nec.spark.planning.aggregation

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.{
  PlanCallsVeFunction,
  SupportsKeyedVeColBatch,
  SupportsVeColBatch,
  VeFunction
}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{
  measureRunningTime,
  registerFunctionCallTime
}
import com.nec.ve.VeRDD.RichKeyedRDDL
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VeHashExchangePlan(exchangeFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction
  with SupportsKeyedVeColBatch {

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  import OriginalCallingContext.Automatic._

  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      withVeLibrary { libRefExchange =>
        logger.info(s"Will map multiple col batches for hash exchange.")
        veColBatches.flatMap { veColBatch =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          try {
            logger.debug(s"Mapping ${veColBatch} for exchange")
            val multiBatches = measureRunningTime(
              veProcess.executeMulti(
                libraryReference = libRefExchange,
                functionName = exchangeFunction.functionName,
                cols = veColBatch.cols,
                results = exchangeFunction.namedResults
              )
            )(registerFunctionCallTime(_, veFunction.functionName))
            logger.debug(s"Mapped to ${multiBatches} completed.")

            val (filled, unfilled) = multiBatches.partition(_._2.head.nonEmpty)
            unfilled.flatMap(_._2).foreach(vcv => vcv.free())
            filled.map { case (p, cl) => p -> VeColBatch.fromList(cl) }
          } finally {
            child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
          }
        }
      }
    }
    .exchangeBetweenVEs(cleanUpInput = true)

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

              multiBatches.map { case (n, l) => n -> VeColBatch.fromList(l) }
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
