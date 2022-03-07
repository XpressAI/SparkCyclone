package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsKeyedVeColBatch, SupportsVeColBatch, VeFunction}
import com.nec.ve.colvector.VeColBatch.VeBatchOfBatches
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

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(VE) ++ batchMetrics("left"
  ) ++ batchMetrics("right") ++ batchMetrics(OUTPUT) ++ batchMetrics("left before exchange"
  ) ++ batchMetrics("right before exchange")

  override def executeVeColumnar(): RDD[VeColBatch] = {
    VeRDD
      .joinExchange(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("left before exchange", b)),
        right = right.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("right before exchange", b)),
        cleanUpInput = true
      )
      .mapPartitions { batchIterator =>
        import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}

        val inputBatches = batchIterator.map { case (left, right) =>
          collectBatchMetrics("left", left)
          collectBatchMetrics("right", right)
          VeColBatch(left.numRows, left.cols ++ right.cols)
        }.toList

        inputBatches match {
          case Nil => Iterator.empty
          case _ =>
            val batches = VeBatchOfBatches.fromVeColBatches(inputBatches)

            withInvocationMetrics(PLAN){
              withVeLibrary { libRefJoin =>
                import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
                val outputBatches = try {
                  withInvocationMetrics(VE){
                    ImplicitMetrics.processMetrics.measureRunningTime {
                      veProcess.executeMultiInOut(
                        libraryReference = libRefJoin,
                        functionName = joinFunction.functionName,
                        batches = batches,
                        results = joinFunction.namedResults
                      )
                    }(ImplicitMetrics.processMetrics.registerFunctionCallTime(_, veFunction.functionName))
                  }
                } finally {
                  inputBatches.foreach(dataCleanup.cleanup(_))
                }
                outputBatches.map(VeColBatch.fromList)
              }
            }.iterator
        }
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
