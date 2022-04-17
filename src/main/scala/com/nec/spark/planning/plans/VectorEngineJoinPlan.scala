package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics
import com.nec.spark.planning._
import com.nec.colvector.VeColBatch.VeBatchOfBatches
import com.nec.ve.{VeColBatch, VeRDDOps}
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
    VeRDDOps
      .joinExchange(
        left = left.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("left before exchange", b)),
        right = right.asInstanceOf[SupportsKeyedVeColBatch].executeVeColumnarKeyed().mapPartitions(b => collectBatchMetrics("right before exchange", b)),
        cleanUpInput = true
      )
      .mapPartitions { tupleIterator =>
        import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}

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
              val leftBatchesBatch = VeBatchOfBatches.fromVeColBatches(leftBatches)
              val rightBatchesBatch = VeBatchOfBatches.fromVeColBatches(rightBatches)

              withVeLibrary { libRefJoin =>
                import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
                val outputBatch = try {
                  withInvocationMetrics(VE){
                    ImplicitMetrics.processMetrics.measureRunningTime {
                      veProcess.executeJoin(
                        libraryReference = libRefJoin,
                        functionName = joinFunction.functionName,
                        left = leftBatchesBatch,
                        right = rightBatchesBatch,
                        results = joinFunction.namedResults
                      )
                    }(ImplicitMetrics.processMetrics.registerFunctionCallTime(_, veFunction.functionName))
                  }
                } finally {
                  leftBatches.foreach(dataCleanup.cleanup(_))
                  rightBatches.foreach(dataCleanup.cleanup(_))
                }

                Iterator.single(VeColBatch.fromList(outputBatch))
              }
          }
        }
      }
  }

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(joinFunction = f(joinFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def veFunction: VeFunction = joinFunction
}
