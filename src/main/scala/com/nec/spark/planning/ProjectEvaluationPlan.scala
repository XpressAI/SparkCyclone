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
package com.nec.spark.planning

import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.spark.planning.ProjectEvaluationPlan.ProjectionContext
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeColVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths
import scala.language.dynamics

final case class ProjectEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  veFunction: VeFunction,
  child: SparkPlan
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private[planning] val projectionContext = ProjectionContext(outputExpressions, child.outputSet)
  import projectionContext._

  override def executeVeColumnar(): RDD[VeColBatch] =
    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        val libRef = veProcess.loadLibrary(Paths.get(veFunction.libraryPath))
        veColBatches.map { veColBatch =>
          import SparkCycloneExecutorPlugin.veProcess
          try {
            val canPassThroughall = passThroughRefs.size == outputExpressions.size
            val cols =
              if (canPassThroughall) Nil
              else
                veProcess.execute(
                  libraryReference = libRef,
                  functionName = veFunction.functionName,
                  cols = veColBatch.cols,
                  results = veFunction.results
                )
            val outBatch = createOutputBatch(cols, veColBatch)

            if (veColBatch.numRows < outBatch.numRows)
              println(s"Input rows = ${veColBatch.numRows}, output = ${outBatch}")
            outBatch
          } finally child
            .asInstanceOf[SupportsVeColBatch]
            .dataCleanup
            .cleanup(ProjectEvaluationPlan.getBatchForPartialCleanup(idsToPass)(veColBatch))
        }
      }

}

object ProjectEvaluationPlan {

  final case class ProjectionContext(
    outputExpressions: Seq[NamedExpression],
    childOutputSet: AttributeSet
  ) {

    val passThroughRefs: Seq[NamedExpression] =
      outputExpressions.filter(_.isInstanceOf[AttributeReference])

    val idsToPass: Seq[Int] = passThroughRefs
      .map(ref =>
        childOutputSet.toList.zipWithIndex.find(findRef => findRef._1.exprId == ref.exprId)
      )
      .collect { case Some((_, id)) =>
        id
      }

    def createOutputBatch(
      calculatedColumns: Seq[VeColVector],
      originalBatch: VeColBatch
    ): VeColBatch = {

      val outputColumns = outputExpressions
        .foldLeft((0, 0, Seq.empty[VeColVector])) {
          case ((calculatedIdx, copiedIdx, seq), a @ AttributeReference(_, _, _, _))
              if childOutputSet.contains(a) =>
            (calculatedIdx, copiedIdx + 1, seq :+ originalBatch.cols(idsToPass(copiedIdx)))

          case ((calculatedIdx, copiedIdx, seq), _) =>
            (calculatedIdx + 1, copiedIdx, seq :+ calculatedColumns(calculatedIdx))
        }
        ._3
        .toList

      VeColBatch.fromList(outputColumns)
    }
  }

  def getBatchForPartialCleanup(idsToPass: Seq[Int])(veColBatch: VeColBatch): VeColBatch = {
    val colsToCleanUp = veColBatch.cols.zipWithIndex
      .collect {
        case (col, idx) if !idsToPass.contains(idx) => col
      }
    VeColBatch.fromList(colsToCleanUp)
  }

}
