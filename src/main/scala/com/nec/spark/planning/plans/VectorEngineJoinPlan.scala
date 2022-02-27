package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.cycloneMetrics
import com.nec.spark.planning.{PlanCallsVeFunction, SupportsKeyedVeColBatch, SupportsVeColBatch, VeFunction}
import com.nec.ve.{VeColBatch, VeRDD}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

import scala.concurrent.duration.NANOSECONDS

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

  override lazy val metrics = Map(
    "execTime" -> SQLMetrics.createTimingMetric(sparkContext, "execution time")
  )

  override def executeVeColumnar(): RDD[VeColBatch] = {
    val execMetric = longMetric("execTime")

    VeRDD
      .joinExchange(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed(),
        right = right.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed(),
        cleanUpInput = true
      )
      .map { case (leftColBatch, rightColBatch) =>
        import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
        val beforeExec = System.nanoTime()

        val res = withVeLibrary { libRefJoin =>
          logger.debug(s"Mapping ${leftColBatch} / ${rightColBatch} for join")
          import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
          val batch =
            try {
              cycloneMetrics.measureRunningTime(
                veProcess.execute(
                  libraryReference = libRefJoin,
                  functionName = joinFunction.functionName,
                  cols = leftColBatch.cols ++ rightColBatch.cols,
                  results = joinFunction.namedResults
                )
              )(cycloneMetrics.registerFunctionCallTime(_, veFunction.functionName))
            } finally {
              dataCleanup.cleanup(leftColBatch)
              dataCleanup.cleanup(rightColBatch)
            }
          logger.debug(s"Completed ${leftColBatch} / ${rightColBatch} => ${batch}.")
          VeColBatch.fromList(batch)
        }
        execMetric += NANOSECONDS.toMillis(System.nanoTime() - beforeExec)
        res
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
