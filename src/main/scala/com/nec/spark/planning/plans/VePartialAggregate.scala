package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsVeColBatch, VeFunction}
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{
  measureRunningTime,
  registerFunctionCallTime
}
import com.nec.ve.VeColBatch
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class VePartialAggregate(
  expectedOutputs: Seq[NamedExpression],
  partialFunction: VeFunction,
  child: SparkPlan
) extends UnaryExecNode
  with SupportsVeColBatch
  with LazyLogging
  with PlanCallsVeFunction {

  require(
    expectedOutputs.size == partialFunction.results.size,
    s"Expected outputs ${expectedOutputs.size} to match final function results size, but got ${partialFunction.results.size}"
  )

  override def executeVeColumnar(): RDD[VeColBatch] = child
    .asInstanceOf[SupportsVeColBatch]
    .executeVeColumnar()
    .mapPartitions { veColBatches =>
      withVeLibrary { libRef =>
        logger.info(s"Will map partial aggregates using $partialFunction")
        veColBatches.map { veColBatch =>
          import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
          logger.debug(s"Mapping a VeColBatch $veColBatch")
          VeColBatch.fromList {
            import OriginalCallingContext.Automatic._
            try {
              val result = measureRunningTime(
                veProcess.execute(
                  libraryReference = libRef,
                  functionName = partialFunction.functionName,
                  cols = veColBatch.cols,
                  results = partialFunction.namedResults
                )
              )(registerFunctionCallTime(_, veFunction.functionName))
              logger.debug(s"Mapped $veColBatch to $result")
              result
            } finally child.asInstanceOf[SupportsVeColBatch].dataCleanup.cleanup(veColBatch)
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
