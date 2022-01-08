package com.nec.spark.planning.aggregation

import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeRDD.RichKeyedRDDL
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.time.{Duration, Instant}

case class VeHashExchange(exchangeFunction: VeFunction, child: SparkPlan)
  extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      withVeLibrary { libRefExchange =>
        logger.info(s"Will map multiple col batches for hash exchange.")
        veColBatches.flatMap { veColBatch =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          try {
            val startTime = Instant.now()
            logger.debug(s"Mapping ${veColBatch} for exchange")
            val multiBatches = veProcess.executeMulti(
              libraryReference = libRefExchange,
              functionName = exchangeFunction.functionName,
              cols = veColBatch.cols,
              results = exchangeFunction.results
            )
            logger.debug(s"Mapped to ${multiBatches} completed.")

            val (filled, unfilled) = multiBatches.partition(_._2.head.nonEmpty)
            unfilled.flatMap(_._2).foreach(vcv => vcv.free())
            val timeTaken = Duration.between(startTime, Instant.now())
            filled
          } finally {
            child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
          }
        }
      }
    }
    .exchangeBetweenVEs(cleanUpInput = true)
    .mapPartitions(f = _.map(lv => VeColBatch.fromList(lv)), preservesPartitioning = true)

  override def output: Seq[Attribute] = child.output

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(exchangeFunction = f(exchangeFunction))

  override def veFunction: VeFunction = exchangeFunction
}
