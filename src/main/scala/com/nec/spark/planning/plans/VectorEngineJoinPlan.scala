package com.nec.spark.planning.plans

import com.nec.spark.planning.{
  PlanCallsVeFunction,
  SupportsKeyedVeColBatch,
  SupportsVeColBatch,
  VeFunction
}
import com.nec.ve.{VeColBatch, VeRDD}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{
  measureRunningTime,
  registerFunctionCallTime
}
import com.nec.ve.VeProcess.OriginalCallingContext

case class VectorEngineJoinPlan(
  outputExpressions: Seq[NamedExpression],
  joinFunction: VeFunction,
  left: SparkPlan,
  right: SparkPlan
) extends SparkPlan
  with BinaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanCallsVeFunction {

  override def executeVeColumnar(): RDD[VeColBatch] =
    VeRDD
      .joinExchange(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed(),
        right = right.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed(),
        cleanUpInput = true
      )(OriginalCallingContext.Automatic.originalCallingContext)
      .map { case (leftColBatch, rightColBatch) =>
        import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
        import com.nec.spark.SparkCycloneExecutorPlugin.source
        withVeLibrary { libRefJoin =>
          logger.debug(s"Mapping ${leftColBatch} / ${rightColBatch} for join")
          import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
          val batch =
            try {
              measureRunningTime(
                veProcess.execute(
                  libraryReference = libRefJoin,
                  functionName = joinFunction.functionName,
                  cols = leftColBatch.cols ++ rightColBatch.cols,
                  results = joinFunction.namedResults
                )
              )(registerFunctionCallTime(_, veFunction.functionName))
            } finally {
              dataCleanup.cleanup(leftColBatch)
              dataCleanup.cleanup(rightColBatch)
            }
          logger.debug(s"Completed ${leftColBatch} / ${rightColBatch} => ${batch}.")
          VeColBatch.fromList(batch)
        }
      }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
