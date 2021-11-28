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
import com.nec.spark.agile.CFunctionGeneration.VeType
import com.nec.spark.planning.OneStageEvaluationPlan.VeFunction
import com.nec.ve.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths
import scala.language.dynamics

object OneStageEvaluationPlan {
  final case class VeFunction(libraryPath: String, functionName: String, results: List[VeType])
}

final case class OneStageEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  veFunction: VeFunction,
  child: SparkPlan
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch {

  override def supportsColumnar: Boolean = true

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeVeColumnar(): RDD[VeColBatch] =
    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        val libRef = veProcess.loadLibrary(Paths.get(veFunction.libraryPath))
        veColBatches.map { veColBatch =>
          import SparkCycloneExecutorPlugin.veProcess
          try {
            val cols = veProcess.execute(
              libraryReference = libRef,
              functionName = veFunction.functionName,
              cols = veColBatch.cols,
              results = veFunction.results
            )

            val outBatch = VeColBatch.fromList(cols)
            if (veColBatch.numRows < outBatch.numRows)
              println(s"Input rows = ${veColBatch.numRows}, output = ${outBatch}")
            outBatch
          } finally veColBatch.cols.foreach(_.free())
        }
      }
}
