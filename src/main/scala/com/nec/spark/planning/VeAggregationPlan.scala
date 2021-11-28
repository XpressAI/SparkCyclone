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
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeBatchOfBatches
import com.nec.ve.VeRDD.RichKeyedRDDL
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

import java.nio.file.Paths
import scala.language.dynamics

final case class VeAggregationPlan(
  outputExpressions: Seq[NamedExpression],
  partialFunction: VeFunction,
  exchangeFunction: VeFunction,
  finalFunction: VeFunction,
  child: SparkPlan
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch {

  override def supportsColumnar: Boolean = true

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def executeVeColumnar(): RDD[VeColBatch] = {
    child
      .asInstanceOf[SupportsVeColBatch]
      .executeVeColumnar()
      .mapPartitions { veColBatches =>
        val libRef = veProcess.loadLibrary(Paths.get(partialFunction.libraryPath))
        val libRefExchange = veProcess.loadLibrary(Paths.get(exchangeFunction.libraryPath))
        veColBatches.flatMap { veColBatch =>
          import SparkCycloneExecutorPlugin.veProcess
          val cols = veProcess.execute(
            libraryReference = libRef,
            functionName = partialFunction.functionName,
            cols = veColBatch.cols,
            results = partialFunction.results
          )

          veProcess.executeMulti(
            libraryReference = libRefExchange,
            functionName = exchangeFunction.functionName,
            cols = cols,
            results = exchangeFunction.results
          )
        }
      }
      .exchangeBetweenVEs()
      .mapPartitions(iteratorBatches =>
        Iterator
          .continually {
            val libRefFinal = veProcess.loadLibrary(Paths.get(finalFunction.libraryPath))
            veProcess.executeMultiIn(
              libraryReference = libRefFinal,
              functionName = exchangeFunction.functionName,
              batches = VeBatchOfBatches
                .fromVeColBatches(
                  iteratorBatches.map(l => VeColBatch(numRows = l.head.numItems, cols = l)).toList
                ),
              results = exchangeFunction.results
            )
          }
          .take(1)
          .map { r => VeColBatch(numRows = r.head.numItems, cols = r) }
      )
  }

}

object VeAggregationPlan {}
