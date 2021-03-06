package io.sparkcyclone.spark.plans

import io.sparkcyclone.data.vector.{VeBatchOfBatches, VeColBatch}
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin.{source, veProcess, vectorEngine}
import io.sparkcyclone.spark.transformation._
import io.sparkcyclone.util.CallContextOps._
import io.sparkcyclone.rdd.VeRDDOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}

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
    initializeMetrics()

    VeRDDOps
      .joinExchange(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("left before exchange", b)),
        right = right.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("right before exchange", b)),
        cleanUpInput = true
      )
      .mapPartitions { tupleIterator =>
        withInvocationMetrics(PLAN){
          val (leftBatchesIter, rightBatchesIter) = tupleIterator.fold((Seq.empty, Seq.empty)){ case ((accLeft, accRight), (left, right)) =>
            (accLeft ++ left, accRight ++ right)
          }

          val leftBatches = leftBatchesIter.toList
          val rightBatches = rightBatchesIter.toList

          collectBatchMetrics("left", leftBatches.iterator)
          collectBatchMetrics("left", rightBatches.iterator)

          (leftBatches, rightBatches) match {
            case (Nil, _) => Iterator.empty
            case (_, Nil) => Iterator.empty
            case _ =>
              val leftBatchesBatch = VeBatchOfBatches(leftBatches)
              val rightBatchesBatch = VeBatchOfBatches(rightBatches)

              withVeLibrary { libRefJoin =>
                val outputBatch = try {
                  withInvocationMetrics(VE){
                    vectorEngine.executeJoin(
                      libRefJoin,
                      joinFunction.name,
                      leftBatchesBatch,
                      rightBatchesBatch,
                      joinFunction.outputs
                    )
                  }
                } finally {
                  leftBatches.foreach(dataCleanup.cleanup(_))
                  rightBatches.foreach(dataCleanup.cleanup(_))
                }

                Iterator.single(VeColBatch(outputBatch))
              }
          }
        }
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction

  override def withNewChildrenInternal(newLeft: SparkPlan, newRight: SparkPlan): VectorEngineJoinPlan = {
    copy(left = newLeft, right = newRight)
  }
}
