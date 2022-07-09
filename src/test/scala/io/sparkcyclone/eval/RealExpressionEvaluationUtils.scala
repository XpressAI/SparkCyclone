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
package io.sparkcyclone.eval

import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.spark.codegen.CFunctionGeneration._
import io.sparkcyclone.native.code._
import io.sparkcyclone.spark.codegen.filter.{FilterFunction, VeFilter}
import io.sparkcyclone.spark.codegen.join.JoinUtils._
import io.sparkcyclone.spark.codegen.projection.ProjectionFunction
import io.sparkcyclone.spark.codegen.sort.{SortFunction, VeSortExpression}
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.native.compiler.VeKernelInfra
import io.sparkcyclone.vectorengine.{VeProcess, VectorEngine}
import io.sparkcyclone.eval.StaticTypingTestAdditions._
import org.apache.arrow.memory.RootAllocator
import com.typesafe.scalalogging.LazyLogging

object RealExpressionEvaluationUtils extends LazyLogging {
  def evalInnerJoin[Input, Output](
    input: List[Input],
    leftKey: TypedCExpression2,
    rightKey: TypedCExpression2,
    output: List[NamedJoinExpression]
  )(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    vectorEngine: VectorEngine,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Output] = {
    val cFunction =
      renderInnerJoin(
        VeInnerJoin(
          inputs = veAllocator.makeCVectors,
          leftKey = leftKey,
          rightKey = rightKey,
          outputs = output
        )
      )

    import io.sparkcyclone.util.CallContextOps._
    evalFunction(cFunction, "project_f")(input, veRetriever.makeCVectors)
  }

  def evalProject[Input, Output](input: List[Input])(expressions: CExpression*)(implicit
    veProcess: VeProcess,
    vectorEngine: VectorEngine,
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Output] = {
    val outputs = veRetriever.veTypes.zip(expressions.toList).zipWithIndex.collect {
      case ((veScalarType: VeScalarType, exp), idx) =>
        NamedTypedCExpression(s"output_${idx}", veScalarType, exp)
      case other => sys.error(s"Not supported/used: ${other}")
    }
    val projectionFn = ProjectionFunction("project_f", veAllocator.makeCVectors, outputs.map(Right(_)))

    import io.sparkcyclone.util.CallContextOps._
    evalFunction(projectionFn.toCFunction)(input, outputs.map(_.cVector))
  }

  def evalFunction[Input, Output](
    cFunction: CFunction,
    functionName: String
  )(input: List[Input], outputs: List[CVector])(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    vectorEngine: VectorEngine,
    veKernelInfra: VeKernelInfra,
    context: CallContext,
    veColVectorSource: VeColVectorSource
  ): Seq[Output] = {
    implicit val allocator = new RootAllocator(Integer.MAX_VALUE)
    veKernelInfra.withCompiled(cFunction.toCodeLinesSPtr(functionName).cCode) { path =>
      val libRef = veProcess.load(path)
      val inputVectors = veAllocator.allocate(input: _*)
      try {
        val resultingVectors =
          vectorEngine.execute(libRef, functionName, inputVectors.columns.toList, outputs)
        veRetriever.retrieve(VeColBatch(resultingVectors))
      } finally inputVectors.free()
    }
  }
  def evalFunction[Input, Output](
    cFunction: CFunction2
  )(input: List[Input], outputs: List[CVector])(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    vectorEngine: VectorEngine,
    veKernelInfra: VeKernelInfra,
    context: CallContext,
    veColVectorSource: VeColVectorSource
  ): Seq[Output] = {
    implicit val allocator = new RootAllocator(Integer.MAX_VALUE)
    veKernelInfra.withCompiled(cFunction) { path =>
      val libRef = veProcess.load(path)
      val inputVectors = veAllocator.allocate(input: _*)
      try {
        val resultingVectors =
          vectorEngine.execute(libRef, cFunction.name, inputVectors.columns.toList, outputs)
        veRetriever.retrieve(VeColBatch(resultingVectors))
      } finally inputVectors.free()
    }
  }

  def evalFilter[Data](input: Data*)(condition: CExpression)(implicit
    veAllocator: VeAllocator[Data],
    veRetriever: VeRetriever[Data],
    veProcess: VeProcess,
    vectorEngine: VectorEngine,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Data] = {
    val filterFn = FilterFunction(
      name = "filter_f",
      filter = VeFilter(
        data = veAllocator.makeCVectors,
        condition = condition,
        stringVectorComputations = Nil
      ),
      onVe = false
    )

    import io.sparkcyclone.util.CallContextOps._
    evalFunction(filterFn.toCFunction)(
      input.toList,
      veRetriever.veTypes.zipWithIndex.map { case (t, i) => t.makeCVector(s"out_${i}") }
    )
  }

  def evalSort[Data](input: Data*)(sorts: VeSortExpression*)(implicit
    veAllocator: VeAllocator[Data],
    veRetriever: VeRetriever[Data],
    veProcess: VeProcess,
    vectorEngine: VectorEngine,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Data] = {
    import io.sparkcyclone.util.CallContextOps._

    val sortFn = SortFunction(
      "sort_f",
      veAllocator.makeCVectors.map(_.asInstanceOf[CScalarVector]),
      sorts.toList
    )
    evalFunction(sortFn.toCFunction)(input = input.toList, veRetriever.makeCVectors)
  }
}
