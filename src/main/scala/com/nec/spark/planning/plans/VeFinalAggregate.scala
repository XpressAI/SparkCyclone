package com.nec.spark.planning.plans

import com.nec.colvector.VeColBatch
import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import com.nec.util.CallContextOps._
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.util.CallContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

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

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = {
    initializeMetrics()

    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        withVeLibrary { libRef =>
          incrementInvocations(PLAN)
          veColBatches.map { veColBatch =>
            logger.debug(s"Preparing to final-aggregate a batch... ${veColBatch}")
            collectBatchMetrics(INPUT, veColBatch)
            withInvocationMetrics(BATCH){
              collectBatchMetrics(OUTPUT, VeColBatch({
                withInvocationMetrics(VE){
                  try {
                    vectorEngine.execute(
                      libRef,
                      finalFunction.functionName,
                      veColBatch.columns.toList,
                      finalFunction.namedResults
                    )
                  } finally {
                    logger.debug("Completed a final-aggregate of  a batch...")
                    child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
                  }
                }
              }))
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
