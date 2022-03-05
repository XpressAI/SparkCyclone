package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
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
  with PlanMetrics
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == partialFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${partialFunction.results.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT) ++ partitionMetrics(PLAN)  ++ partitionMetrics(PLAN)

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execResult =     child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()


    execResult.mapPartitionsWithIndex { (index,veColBatches) =>
      incrementInvocations(PLAN)
      collectPartitionMetrics(s"${index}${PLAN}",execResult.getNumPartitions)
      collectPartitionBatchSize(index,veColBatches.size)
      withVeLibrary { libRef =>
        logger.info(s"Will map partial aggregates using $partialFunction")
        veColBatches.map { veColBatch =>
          collectBatchMetrics(INPUT, veColBatch)

          withInvocationMetrics(BATCH) {
            import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
            logger.debug(s"Mapping a VeColBatch $veColBatch")
            collectBatchMetrics(OUTPUT, VeColBatch.fromList {
              import OriginalCallingContext.Automatic._
              try {
                val result = withInvocationMetrics(VE){
                  ImplicitMetrics.processMetrics.measureRunningTime(
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
                }
                logger.debug(s"Mapped $veColBatch to $result")
                result
              } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
            })
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
