package com.nec.spark.planning.plans

import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch}
import com.nec.ve.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths

case class VeFinalAggregate(
  expectedOutputs: Seq[NamedExpression],
  finalFunction: VeFunction,
  child: SparkPlan
) extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == finalFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${finalFunction.results.size}"
  )

  import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      withVeLibrary { libRef =>
        veColBatches.map { veColBatch =>
          logger.debug(s"Preparing to final-aggregate a batch... ${veColBatch}")

          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          VeColBatch.fromList {
            try veProcess.execute(
              libraryReference = libRef,
              functionName = finalFunction.functionName,
              cols = veColBatch.cols,
              results = finalFunction.results
            )
            finally {
              logger.debug("Completed a final-aggregate of  a batch...")
              child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
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
