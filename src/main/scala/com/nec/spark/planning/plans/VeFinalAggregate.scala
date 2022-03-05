package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, source}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
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
  with PlanMetrics
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == finalFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${finalFunction.results.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)  ++ partitionMetrics(PLAN)

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = {
    val res =  child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
    res.mapPartitionsWithIndex { (index,veColBatches) =>
      collectPartitionMetrics(s"${index}${PLAN}",res.getNumPartitions)
      withVeLibrary { libRef =>
        incrementInvocations(PLAN)
        veColBatches.map { veColBatch =>
          collectPartitionBatchSize(index,veColBatch.numRows)
          logger.debug(s"Preparing to final-aggregate a batch... ${veColBatch}")
          collectBatchMetrics(INPUT, veColBatch)
          withInvocationMetrics(BATCH){
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            collectBatchMetrics(OUTPUT, VeColBatch.fromList {
              import OriginalCallingContext.Automatic._
              withInvocationMetrics(VE){
                try ImplicitMetrics.processMetrics.measureRunningTime(
                  veProcess.execute(
                    libraryReference = libRef,
                    functionName = finalFunction.functionName,
                    cols = veColBatch.cols,
                    results = finalFunction.namedResults
                  )
                )(ImplicitMetrics.processMetrics.registerFunctionCallTime(_, veFunction.functionName))
                finally {
                  logger.debug("Completed a final-aggregate of  a batch...")
                  child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
                }
              }
            })
          }
        }
      }
    }

  }

  override def output: Seq[Attribute] = expectedOutputs.map(_.toAttribute)

  override def veFunction: VeFunction = finalFunction

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(finalFunction = f(finalFunction))
}
