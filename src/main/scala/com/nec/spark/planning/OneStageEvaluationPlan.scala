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

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import scala.language.dynamics

//noinspection DuplicatedCode
final case class OneStageEvaluationPlan(
  outputExpressions: Seq[NamedExpression],
  functionName: String,
  cFunction: CFunction,
  child: SparkPlan,
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with UnaryExecNode
  with LazyLogging
  with SupportsArrowColumns {

  require(outputExpressions.nonEmpty, "Expected OutputExpressions to be non-empty")

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def executeColumnWise: RDD[ColumnarBatch] = {
    val evaluator = nativeEvaluator.forCode(
      List(cFunction.toCodeLinesPF(functionName)).reduce(_ ++ _).lines.mkString("\n", "\n", "\n")
    )

    logger.debug(s"Will execute columnar NewCEvaluationPlan for child ${child}; ${child.output}")

    getChildSkipMappings()
      .executeColumnar()
      .map { batch =>
        implicit val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
          .newChildAllocator(s"Writer for partial collector", 0, Long.MaxValue)

        val inputVectors = child.output.indices
          .map(n =>
            batch
              .column(n)
              .getArrowValueVector
          )

        val partialOutputVectors: List[ValueVector] =
          cFunction.outputs.map(CFunctionGeneration.allocateFrom(_))
        try {
          val outputArgs = inputVectors.toList.map(_ => None) ++
            partialOutputVectors.map(v => Some(SupportedVectorWrapper.wrapOutput(v)))
          val inputArgs = inputVectors.toList.map(iv =>
            Some(SupportedVectorWrapper.wrapInput(iv))
          ) ++ partialOutputVectors.map(_ => None)

          evaluator.callFunction(
            name = functionName,
            inputArguments = inputArgs,
            outputArguments = outputArgs
          )
          val vectors = partialOutputVectors
            .map(vector => new ArrowColumnVector(vector))
          val columnarBatch =
            new ColumnarBatch(vectors.toArray)
          vectors.headOption
            .map(_.getArrowValueVector.getValueCount)
            .foreach(columnarBatch.setNumRows)
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            columnarBatch.close()
          }
          columnarBatch
        } finally {
          inputVectors.foreach(_.close())
        }
      }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    sys.error("Not supported row evaluation")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    executeColumnWise
}
