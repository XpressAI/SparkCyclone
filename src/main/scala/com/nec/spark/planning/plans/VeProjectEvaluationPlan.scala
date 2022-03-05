/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark.planning.plans

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.{ImplicitMetrics, source, veProcess}
import com.nec.spark.planning.{PlanCallsVeFunction, PlanMetrics, SupportsVeColBatch, VeFunction}
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import scala.concurrent.duration.NANOSECONDS
import scala.language.dynamics

final case class VeProjectEvaluationPlan(
                                          outputExpressions: Seq[NamedExpression],
                                          veFunction: VeFunction,
                                          child: SparkPlan
                                        ) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch
  with PlanMetrics
  with PlanCallsVeFunction {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override lazy val metrics = invocationMetrics(PLAN) ++ invocationMetrics(BATCH) ++ invocationMetrics(VE) ++ batchMetrics(INPUT) ++ batchMetrics(OUTPUT)  ++ partitionMetrics(PLAN)

  override def updateVeFunction(f: VeFunction => VeFunction): SparkPlan =
    copy(veFunction = f(veFunction))

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private val projectionContext =
    VeProjectEvaluationPlan.ProjectionContext(outputExpressions, child.outputSet.toList)

  import projectionContext._
  override def executeVeColumnar(): RDD[VeColBatch] = {
    val res =    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
    res.mapPartitionsWithIndex { (index,veColBatches) =>
      collectPartitionMetrics(s"${index}PLAN",res.getNumPartitions)
      incrementInvocations(PLAN)
      withVeLibrary { libRef =>
        withInvocationMetrics(BATCH){
          veColBatches.map { veColBatch =>
            collectBatchMetrics(INPUT, veColBatch)
            import OriginalCallingContext.Automatic._
            import SparkCycloneExecutorPlugin.veProcess
            try {
              val canPassThroughall = columnIndicesToPass.size == outputExpressions.size

              val cols =
                if (canPassThroughall) Nil
                else {
                  withInvocationMetrics(VE){
                    ImplicitMetrics.processMetrics.measureRunningTime(
                      veProcess.execute(
                        libraryReference = libRef,
                        functionName = veFunction.functionName,
                        cols = veColBatch.cols,
                        results = veFunction.namedResults
                      )
                    )(
                      ImplicitMetrics.processMetrics
                        .registerFunctionCallTime(_, veFunction.functionName)
                    )
                  }

                }
              val outBatch = createOutputBatch(cols, veColBatch)

              if (veColBatch.numRows < outBatch.numRows)
                println(s"Input rows = ${veColBatch.numRows}, output = ${outBatch}")
              collectBatchMetrics(OUTPUT, outBatch)
            } finally {
              child
                .asInstanceOf[SupportsVeColBatch]
                .dataCleanup
                .cleanup(
                  VeProjectEvaluationPlan
                    .getBatchForPartialCleanup(columnIndicesToPass)(veColBatch)
                )
            }
          }
        }
      }
    }
  }
}

object VeProjectEvaluationPlan {

  private[planning] final case class ProjectionContext(
                                                        outputExpressions: Seq[NamedExpression],
                                                        inputs: List[NamedExpression]
                                                      ) {

    val columnIndicesToPass: Seq[Int] = outputExpressions
      .filter(_.isInstanceOf[AttributeReference])
      .map(ref => inputs.zipWithIndex.find(findRef => findRef._1.exprId == ref.exprId))
      .collect { case Some((_, id)) =>
        id
      }

    def createOutputBatch(
                           calculatedColumns: Seq[VeColVector],
                           originalBatch: VeColBatch
                         ): VeColBatch = {
      //      println(s"""Inputs ${inputs}, Outputs ${outputExpressions}""")
      val outputColumns = outputExpressions
        .foldLeft((0, 0, Seq.empty[VeColVector])) {
          case ((calculatedIdx, copiedIdx, seq), a @ AttributeReference(_, _, _, _))
            if inputs.exists(ex => ex.exprId == a.exprId) =>
            (
              calculatedIdx,
              copiedIdx + 1,
              seq :+ originalBatch.cols(columnIndicesToPass(copiedIdx))
            )

          case ((calculatedIdx, copiedIdx, seq), ex) =>
            (calculatedIdx + 1, copiedIdx, seq :+ calculatedColumns(calculatedIdx))
        }
        ._3
        .toList

      require(
        outputColumns.forall(_.numItems == originalBatch.numRows),
        s"Expected all output columns to have size ${originalBatch.numRows}, but got: ${outputColumns}"
      )

      VeColBatch.fromList(outputColumns)
    }
  }

  def getBatchForPartialCleanup(idsToPass: Seq[Int])(veColBatch: VeColBatch): VeColBatch = {
    val colsToCleanUp = veColBatch.cols.zipWithIndex
      .collect {
        case (col, idx) if !idsToPass.contains(idx) => col
      }
    if (!colsToCleanUp.isEmpty) VeColBatch.fromList(colsToCleanUp) else VeColBatch.empty
  }

}
