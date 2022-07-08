package io.sparkcyclone.spark.planning.plans

import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import io.sparkcyclone.util.CallContext
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
    expectedOutputs.size == finalFunction.outputs.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${finalFunction.outputs.size}"
  )

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)

  import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.veProcess
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
                      finalFunction.name,
                      veColBatch.columns.toList,
                      finalFunction.outputs
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
