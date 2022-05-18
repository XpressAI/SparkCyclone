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
package com.nec.ve.eval

import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.core._
import com.nec.spark.agile.filter.FilterFunction
import com.nec.spark.agile.join.JoinUtils._
import com.nec.spark.agile.projection.ProjectionFunction
import com.nec.spark.agile.sort.{SortFunction, VeSortExpression}
import com.nec.util.CallContext
import com.nec.ve._
import com.nec.colvector.{VeColBatch, VeColVectorSource}
import com.nec.ve.eval.StaticTypingTestAdditions._
import org.apache.arrow.memory.RootAllocator
import com.typesafe.scalalogging.LazyLogging

object RealExpressionEvaluationUtils extends LazyLogging {

  def evalAggregate[Input, Output](
    input: List[Input]
  )(expressions: NamedGroupByExpression*)(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Output] = {
    val cFunction =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = veAllocator.makeCVectors,
          groups = Nil,
          outputs = expressions.map(e => Right(e)).toList
        )
      ).renderGroupBy

    import com.nec.util.CallContextOps._
    evalFunction(cFunction, "agg")(input, veRetriever.makeCVectors)
  }

  def evalInnerJoin[Input, Output](
    input: List[Input],
    leftKey: TypedCExpression2,
    rightKey: TypedCExpression2,
    output: List[NamedJoinExpression]
  )(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
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

    import com.nec.util.CallContextOps._
    evalFunction(cFunction, "project_f")(input, veRetriever.makeCVectors)
  }

  def evalGroupBySum[Input, Output](
    input: List[Input],
    groups: List[Either[StringGrouping, TypedCExpression2]],
    expressions: List[Either[NamedStringProducer, NamedGroupByExpression]]
  )(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Output] = {
    val cFunction =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(inputs = veAllocator.makeCVectors, groups = groups, outputs = expressions)
      ).renderGroupBy

    import com.nec.util.CallContextOps._
    evalFunction(cFunction, "project_f")(input, veRetriever.makeCVectors)

  }

  def evalGroupBySumStr[Input, Output](input: List[Input])(
    groups: (StringGrouping, TypedCExpression2)
  )(expressions: List[Either[NamedStringProducer, NamedGroupByExpression]])(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veColVectorSource: VeColVectorSource,
    veProcess: VeProcess,
    veKernelInfra: VeKernelInfra
  ): Seq[Output] = {
    val cFunction =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = veAllocator.makeCVectors,
          groups = List(Left(groups._1), Right(groups._2)),
          outputs = expressions
        )
      ).renderGroupBy

    import com.nec.util.CallContextOps._
    evalFunction(cFunction, "project_f")(input, veRetriever.makeCVectors)
  }

  def evalProject[Input, Output](input: List[Input])(expressions: CExpression*)(implicit
    veProcess: VeProcess,
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

    import com.nec.util.CallContextOps._
    evalFunction(projectionFn.toCFunction)(input, outputs.map(_.cVector))
  }

  def evalFunction[Input, Output](
    cFunction: CFunction,
    functionName: String
  )(input: List[Input], outputs: List[CVector])(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    veKernelInfra: VeKernelInfra,
    context: CallContext,
    veColVectorSource: VeColVectorSource
  ): Seq[Output] = {
    implicit val allocator = new RootAllocator(Integer.MAX_VALUE)
    veKernelInfra.withCompiled(cFunction.toCodeLinesSPtr(functionName).cCode) { path =>
      val libRef = veProcess.loadLibrary(path)
      val inputVectors = veAllocator.allocate(input: _*)
      try {
        val resultingVectors =
          veProcess.execute(libRef, functionName, inputVectors.columns.toList, outputs)
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
    veKernelInfra: VeKernelInfra,
    context: CallContext,
    veColVectorSource: VeColVectorSource
  ): Seq[Output] = {
    implicit val allocator = new RootAllocator(Integer.MAX_VALUE)
    veKernelInfra.compiledWithHeaders(cFunction) { path =>
      val libRef = veProcess.loadLibrary(path)
      val inputVectors = veAllocator.allocate(input: _*)
      try {
        val resultingVectors =
          veProcess.execute(libRef, cFunction.name, inputVectors.columns.toList, outputs)
        veRetriever.retrieve(VeColBatch(resultingVectors))
      } finally inputVectors.free()
    }
  }

  def evalFilter[Data](input: Data*)(condition: CExpression)(implicit
    veAllocator: VeAllocator[Data],
    veRetriever: VeRetriever[Data],
    veProcess: VeProcess,
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

    import com.nec.util.CallContextOps._
    evalFunction(filterFn.toCFunction)(
      input.toList,
      veRetriever.veTypes.zipWithIndex.map { case (t, i) => t.makeCVector(s"out_${i}") }
    )
  }

  def evalSort[Data](input: Data*)(sorts: VeSortExpression*)(implicit
    veAllocator: VeAllocator[Data],
    veRetriever: VeRetriever[Data],
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): Seq[Data] = {
    import com.nec.util.CallContextOps._

    val sortFn = SortFunction(
      "sort_f",
      veAllocator.makeCVectors.map(_.asInstanceOf[CScalarVector]),
      sorts.toList
    )
    evalFunction(sortFn.toCFunction)(input = input.toList, veRetriever.makeCVectors)
  }
}
