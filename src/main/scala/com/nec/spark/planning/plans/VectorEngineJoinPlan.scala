package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsKeyedVeColBatch, SupportsVeColBatch, VeFunction}
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
  with PlanMetrics
  with PlanCallsVeFunction {

  override lazy val metrics = invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics("left"
  ) ++ batchMetrics("right") ++ batchMetrics(OUTPUT) ++ batchMetrics("left before exchange"
  ) ++ batchMetrics("right before exchange")

  override def executeVeColumnar(): RDD[VeColBatch] = {
    VeRDD
      .joinExchange(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("left before exchange", b)),
        right = right.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("right before exchange", b)),
        cleanUpInput = true
      )
      .map { case (leftColBatch, rightColBatch) =>
        import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
        collectBatchMetrics("left", leftColBatch)
        collectBatchMetrics("right", rightColBatch)

        withInvocationMetrics(BATCH) {
          withVeLibrary { libRefJoin =>
            logger.debug(s"Mapping ${leftColBatch} / ${rightColBatch} for join")
            import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
            val batch =
              try {
                withInvocationMetrics(VE){
                  ImplicitMetrics.processMetrics.measureRunningTime {
                    veProcess.execute(
                      libraryReference = libRefJoin,
                      functionName = joinFunction.functionName,
                      cols = leftColBatch.cols ++ rightColBatch.cols,
                      results = joinFunction.namedResults
                    )
                  }(ImplicitMetrics.processMetrics.registerFunctionCallTime(_, veFunction.functionName))
                }
              } finally {
                dataCleanup.cleanup(leftColBatch)
                dataCleanup.cleanup(rightColBatch)
              }
            logger.debug(s"Completed ${leftColBatch} / ${rightColBatch} => ${batch}.")
            collectBatchMetrics(OUTPUT, VeColBatch.fromList(batch))

          }
        }
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
