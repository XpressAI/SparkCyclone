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
package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.WithTestAllocator
import com.nec.cmake.eval.OldUnifiedGroupByFunctionGeneration
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration.JoinExpression.JoinProjection
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{veNullableDouble, VeNullableDouble}
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression.EvalFallback
import com.nec.spark.agile.{DeclarativeAggregationConverter, StringProducer}
import com.nec.ve.StaticTypingTestAdditions._
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{Corr, Sum}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
final class RealExpressionEvaluationSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {

  private implicit val fallback: EvalFallback = EvalFallback.noOp
  import RealExpressionEvaluationSpec._
  "We can transform a column" in {
    expect(
      evalProject[Double, (Double, Double)](List[Double](90.0, 1.0, 2, 19, 14))(
        CExpression("2 * input_0->data[i]", None),
        CExpression("2 + input_0->data[i]", None)
      ) == List[(Double, Double)]((180, 92), (2, 3), (4, 4), (38, 21), (28, 16))
    )
  }

  "We can transform a null-column (FilterNull)" in {
    expect(
      evalFilter[Option[Double]](Some(90), None, Some(123))(
        CExpression(cCode = "input_0->data[i] != 90", isNotNullCode = None)
      ) == List[Option[Double]](None, Some(123))
    )
  }

  "We can project a null-column (ProjectNull)" in {
    expect(
      evalProject[Double, (Double, Option[Double])](List[Double](90.0, 1.0, 2, 19, 14))(
        CExpression("2 * input_0->data[i]", None),
        CExpression("2 + input_0->data[i]", Some("input_0->data[i] == 2"))
      ) == List[(Double, Option[Double])](
        (180, None),
        (2, None),
        (4, Some(4)),
        (38, None),
        (28, None)
      )
    )
  }

  "We can transform a null-column" in {
    expect(
      evalProject[Double, (Double, Option[Double])](List[Double](90.0, 1.0, 2, 19, 14))(
        CExpression("2 * input_0->data[i]", None),
        CExpression("2 + input_0->data[i]", Some("0"))
      ) == List[(Double, Option[Double])]((180, None), (2, None), (4, None), (38, None), (28, None))
    )
  }

  "We can filter a column" in {
    expect(
      evalFilter[Double](90.0, 1.0, 2, 19, 14)(
        CExpression(cCode = "input_0->data[i] > 15", isNotNullCode = None)
      ) == List[Double](90, 19)
    )
  }

  "We can filter a column by a String (FilterByString)" ignore {

    /** Ignored because we are likely not going to support filtering * */
    val result = evalFilter[(String, Double)](
      ("x", 90.0),
      ("one", 1.0),
      ("two", 2.0),
      ("prime", 19.0),
      ("other", 14.0)
    )(
      CExpression(
        cCode =
          """std::string(input_0->data, input_0->offsets[i], input_0->offsets[i] + input_0->lengths[i]) == std::string("one")""",
        isNotNullCode = None
      )
    )
    val expected = List[(String, Double)](("one", 1.0))

    expect(result == expected)
  }
  "We can filter a column with a String" ignore {

    /** Ignored because we are likely not going to support filtering * */

    val result = evalFilter[(String, Double)](
      ("x", 90.0),
      ("one", 1.0),
      ("two", 2.0),
      ("prime", 19.0),
      ("other", 14.0)
    )(CExpression(cCode = "input_1->data[i] > 15", isNotNullCode = None))
    val expected = List[(String, Double)](("x", 90.0), ("prime", 19.0))

    expect(result == expected)
  }

  "We can sort" in {
    expect(
      evalSort[(Double, Double)]((90.0, 5.0), (1.0, 4.0), (2.0, 2.0), (19.0, 1.0), (14.0, 3.0))(
        VeSortExpression(
          TypedCExpression2(
            VeScalarType.VeNullableDouble,
            CExpression(cCode = "input_1->data[i]", isNotNullCode = None)
          ),
          Ascending
        )
      ) ==
        List[(Double, Double)](19.0 -> 1.0, 2.0 -> 2.0, 14.0 -> 3.0, 1.0 -> 4.0, 90.0 -> 5.0)
    )
  }

  "We can sort (3 cols)" in {
    val results =
      evalSort[(Double, Double, Double)]((90.0, 5.0, 1.0), (1.0, 4.0, 3.0), (2.0, 2.0, 0.0))(
        VeSortExpression(
          TypedCExpression2(
            VeScalarType.VeNullableDouble,
            CExpression(cCode = "input_2->data[i]", isNotNullCode = None)
          ),
          Ascending
        )
      )
    val expected =
      List[(Double, Double, Double)]((2.0, 2.0, 0.0), (90.0, 5.0, 1.0), (1.0, 4.0, 3.0))
    expect(results == expected)
  }

  "We can sort (3 cols) desc" in {
    val results =
      evalSort[(Double, Double, Double)]((1.0, 4.0, 3.0), (90.0, 5.0, 1.0), (2.0, 2.0, 0.0))(
        VeSortExpression(
          TypedCExpression2(
            VeScalarType.VeNullableDouble,
            CExpression(cCode = "input_2->data[i]", isNotNullCode = None)
          ),
          Descending
        )
      )
    val expected =
      List[(Double, Double, Double)]((1.0, 4.0, 3.0), (90.0, 5.0, 1.0), (2.0, 2.0, 0.0))
    expect(results == expected)
  }

  "We can aggregate / group by on an empty grouping" ignore {
    val result = evalAggregate[(Double, Double, Double), Double](
      List((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0))
    )(
      NamedGroupByExpression(
        "exp",
        veNullableDouble,
        GroupByAggregation(
          Aggregation.sum(CExpression("input_2->data[i] - input_0->data[i]", None))
        )
      )
    )
    assert(result == List[Double](6.6))
  }

  "Average is computed correctly" in {
    val result = evalAggregate[Double, Double](List[Double](1, 2, 3))(
      NamedGroupByExpression(
        "exp",
        veNullableDouble,
        GroupByAggregation(Aggregation.avg(CExpression("input_0->data[i]", None)))
      )
    )
    assert(result == List[Double](2))
  }

  "We can aggregate / group by (simple sum)" in {
    val result = evalGroupBySum[(Double, Double, Double), (Double, Double, Double)](
      List((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0), (3, 4, 9)),
      groups = List(
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None))
        ),
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
        )
      ),
      expressions = List(
        Right(
          NamedGroupByExpression(
            "output_0",
            VeNullableDouble,
            GroupByProjection(CExpression("input_0->data[i]", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_1",
            VeNullableDouble,
            GroupByProjection(CExpression("input_1->data[i] + 1", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_2",
            VeNullableDouble,
            GroupByAggregation(
              Aggregation.sum(CExpression("input_2->data[i] - input_0->data[i]", None))
            )
          )
        )
      )
    )
    assert(
      result ==
        List[(Double, Double, Double)]((1.0, 3.0, 5.0), (1.5, 2.2, 1.6), (3.0, 5.0, 6.0))
    )
  }

  "We can aggregate / group by a String value (GroupByString)" in {
    val input = List[(String, Double)](("x", 1.0), ("yy", 2.0), ("ax", 3.0), ("x", -1.0))
    val expected = input.groupBy(_._1).mapValues(_.map(_._2).sum).toList

    /** SELECT a, SUM(b) group by a, b*b */
    val result =
      evalGroupBySumStr[(String, Double), (String, Double)](input)(
        (
          StringGrouping("input_0"),
          TypedCExpression2(
            VeScalarType.veNullableDouble,
            CExpression("input_1->data[i] * input_1->data[i]", None)
          )
        )
      )(
        List(
          Left(NamedStringProducer("output_0", StringProducer.copyString("input_0"))),
          Right(
            NamedGroupByExpression(
              "output_1",
              VeNullableDouble,
              GroupByAggregation(Aggregation.sum(CExpression("input_1->data[i]", None)))
            )
          )
        )
      )

    assert(result.sorted == expected.sorted)
  }

  "We can aggregate / group by with NULL input check values" in {
    val result = evalGroupBySum[(Double, Double, Double), (Double, Double, Double)](
      input = List((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0)),
      groups = List(
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None))
        ),
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
        )
      ),
      expressions = List(
        Right(
          NamedGroupByExpression(
            "output_0",
            VeNullableDouble,
            GroupByProjection(CExpression("input_0->data[i]", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_1",
            VeNullableDouble,
            GroupByProjection(CExpression("input_1->data[i] + 1", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_2",
            VeNullableDouble,
            GroupByAggregation(
              Aggregation.sum(
                CExpression("input_2->data[i] - input_0->data[i]", Some("input_2->data[i] != 4.0"))
              )
            )
          )
        )
      )
    )
    assert(
      result ==
        List[(Double, Double, Double)]((1.0, 3.0, 2.0), (1.5, 2.2, 1.6))
    )
  }

  "We can aggregate / group by with NULLs for grouped computations" in {
    val result = evalGroupBySum[(Double, Double, Double), (Option[Double], Double, Double)](
      input = List((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0)),
      groups = List(
        Right(
          TypedCExpression2(
            VeScalarType.veNullableDouble,
            CExpression("input_0->data[i]", Some("input_2->data[i] != 4.0"))
          )
        ),
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
        )
      ),
      expressions = List(
        Right(
          NamedGroupByExpression(
            "output_0",
            VeNullableDouble,
            GroupByProjection(CExpression("input_0->data[i]", Some("input_2->data[i] != 4.0")))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_1",
            VeNullableDouble,
            GroupByProjection(CExpression("input_1->data[i] + 1", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_2",
            VeNullableDouble,
            GroupByAggregation(
              Aggregation.sum(CExpression("input_2->data[i] - input_0->data[i]", None))
            )
          )
        )
      )
    )
    assert(
      result ==
        List[(Option[Double], Double, Double)](
          (None, 3.0, 3.0),
          (Some(1.0), 3.0, 2.0),
          (Some(1.5), 2.2, 1.6)
        )
    )
  }

  "We can aggregate / group by with NULLs for inputs as well" in {
    val result =
      evalGroupBySum[(Option[Double], Double, Double), (Option[Double], Double, Option[Double])](
        input = List[(Option[Double], Double, Double)](
          (Some(1.0), 2.0, 3.0),
          (Some(1.5), 1.2, 3.1),
          (None, 2.0, 4.0)
        ),
        groups = List(
          Right(
            TypedCExpression2(
              VeScalarType.veNullableDouble,
              CExpression("input_0->data[i]", Some("input_0->get_validity(i)"))
            )
          ),
          Right(
            TypedCExpression2(
              VeScalarType.veNullableDouble,
              CExpression("input_1->data[i]", Some("input_1->get_validity(i)"))
            )
          )
        ),
        expressions = List(
          Right(
            NamedGroupByExpression(
              "output_0",
              VeNullableDouble,
              GroupByProjection(CExpression("input_0->data[i]", Some("input_0->get_validity(i)")))
            )
          ),
          Right(
            NamedGroupByExpression(
              "output_1",
              VeNullableDouble,
              GroupByProjection(
                CExpression("input_1->data[i] + 1", Some("input_1->get_validity(i)"))
              )
            )
          ),
          Right(
            NamedGroupByExpression(
              "output_2",
              VeNullableDouble,
              GroupByAggregation(
                Aggregation.sum(
                  CExpression(
                    "input_2->data[i] - input_0->data[i]",
                    Some("input_0->get_validity(i) && input_2->get_validity(i)")
                  )
                )
              )
            )
          )
        )
      )
    assert(
      result ==
        List[(Option[Double], Double, Option[Double])](
          (None, 3.0, Some(0.0)),
          (Some(1.0), 3.0, Some(2.0)),
          (Some(1.5), 2.2, Some(1.6))
        )
    )
  }

  "We can sum using DeclarativeAggregate" in {
    val result = evalGroupBySum[(Double, Double, Double), (Double, Double, Double)](
      input = List((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0)),
      groups = List(
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None))
        ),
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
        )
      ),
      expressions = List(
        Right(
          NamedGroupByExpression(
            "output_0",
            VeNullableDouble,
            GroupByProjection(CExpression("input_0->data[i]", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_1",
            VeNullableDouble,
            GroupByProjection(CExpression("input_1->data[i] + 1", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_2",
            VeNullableDouble,
            GroupByAggregation(
              DeclarativeAggregationConverter(
                Sum(AttributeReference("input_0->data[i]", DoubleType)())
              )
            )
          )
        )
      )
    )
    assert(
      result ==
        List[(Double, Double, Double)]((1.0, 3.0, 2.0), (1.5, 2.2, 1.5))
    )
  }

  "We can Inner Join" in {
    val leftKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_0->data[i]", None))

    val rightKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_3->data[i]", None))

    val outputs = (
      TypedJoinExpression[Double](JoinProjection(CExpression("input_1->data[left_out[i]]", None))),
      TypedJoinExpression[Double](JoinProjection(CExpression("input_2->data[right_out[i]]", None))),
      TypedJoinExpression[Double](JoinProjection(CExpression("input_0->data[left_out[i]]", None))),
      TypedJoinExpression[Double](JoinProjection(CExpression("input_3->data[right_out[i]]", None)))
    )

    import JoinExpressor.RichJoin

    val out = evalInnerJoin[(Double, Double, Double, Double), (Double, Double, Double, Double)](
      List(
        (1.0, 2.0, 5.0, 1.0),
        (3.0, 2.0, 3.0, 7.0),
        (11.0, 7.0, 12.0, 11.0),
        (8.0, 2.0, 3.0, 9.0)
      ),
      leftKey,
      rightKey,
      outputs.expressed
    )

    assert(out == List((2.0, 5.0, 1.0, 1.0), (7.0, 12.0, 11.0, 11.0)))
  }

  "We can aggregate / group by (correlation)" in {
    val result = evalGroupBySum[(Double, Double, Double), (Double, Double, Double)](
      input = List((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0), (1.5, 1.2, 4.1)),
      groups = List(
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None))
        ),
        Right(
          TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
        )
      ),
      expressions = List(
        Right(
          NamedGroupByExpression(
            "output_0",
            VeNullableDouble,
            GroupByProjection(CExpression("input_0->data[i]", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_1",
            VeNullableDouble,
            GroupByProjection(CExpression("input_1->data[i] + 1", None))
          )
        ),
        Right(
          NamedGroupByExpression(
            "output_2",
            VeNullableDouble,
            GroupByAggregation(
              DeclarativeAggregationConverter(
                Corr(
                  AttributeReference("input_2->data[i]", DoubleType)(),
                  AttributeReference("input_2->data[i]", DoubleType)()
                )
              )
            )
          )
        )
      )
    )
    assert(
      result ==
        List[(Double, Double, Double)]((1.0, 3.0, 1.0), (1.5, 2.2, 1.0))
    )
  }

}

object RealExpressionEvaluationSpec extends LazyLogging {

  def evalAggregate[Input, Output](
    input: List[Input]
  )(expressions: NamedGroupByExpression*)(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): List[Output] = {
    val cFunction =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = veAllocator.makeCVectors,
          groups = Nil,
          outputs = expressions.map(e => Right(e)).toList
        )
      ).renderGroupBy

    import OriginalCallingContext.Automatic._
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
  ): List[Output] = {
    val cFunction =
      renderInnerJoin(
        VeInnerJoin(
          inputs = veAllocator.makeCVectors,
          leftKey = leftKey,
          rightKey = rightKey,
          outputs = output
        )
      )

    import OriginalCallingContext.Automatic._
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
  ): List[Output] = {
    val cFunction =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(inputs = veAllocator.makeCVectors, groups = groups, outputs = expressions)
      ).renderGroupBy

    import OriginalCallingContext.Automatic._
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
  ): List[Output] = {
    val cFunction =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = veAllocator.makeCVectors,
          groups = List(Left(groups._1), Right(groups._2)),
          outputs = expressions
        )
      ).renderGroupBy

    import OriginalCallingContext.Automatic._
    evalFunction(cFunction, "project_f")(input, veRetriever.makeCVectors)
  }

  def evalProject[Input, Output](input: List[Input])(expressions: CExpression*)(implicit
    veProcess: VeProcess,
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): List[Output] = {
    val functionName = "project_f"

    val outputs = veRetriever.veTypes.zip(expressions.toList).zipWithIndex.collect {
      case ((veScalarType: VeScalarType, exp), idx) =>
        NamedTypedCExpression(s"output_${idx}", veScalarType, exp)
      case other => sys.error(s"Not supported/used: ${other}")
    }

    val cFunction = renderProjection(
      VeProjection(inputs = veAllocator.makeCVectors, outputs = outputs.map(out => Right(out)))
    )

    import OriginalCallingContext.Automatic._
    evalFunction(cFunction, functionName)(input, outputs.map(_.cVector))
  }

  def evalFunction[Input, Output](
    cFunction: CFunction,
    functionName: String
  )(input: List[Input], outputs: List[CVector])(implicit
    veAllocator: VeAllocator[Input],
    veRetriever: VeRetriever[Output],
    veProcess: VeProcess,
    veKernelInfra: VeKernelInfra,
    originalCallingContext: OriginalCallingContext,
    veColVectorSource: VeColVectorSource
  ): List[Output] = {
    WithTestAllocator { implicit allocator =>
      veKernelInfra.withCompiled(cFunction.toCodeLinesSPtr(functionName).cCode) { path =>
        val libRef = veProcess.loadLibrary(path)
        val inputVectors = veAllocator.allocate()
        println(s"Input = ${inputVectors}")
        try {
          val resultingVectors =
            veProcess.execute(libRef, functionName, inputVectors.cols, outputs)
          veRetriever.retrieve(VeColBatch.fromList(resultingVectors))
        } finally inputVectors.free()
      }
    }
  }

  def evalFilter[Data](input: Data*)(condition: CExpression)(implicit
    veAllocator: VeAllocator[Data],
    veRetriever: VeRetriever[Data],
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    veKernelInfra: VeKernelInfra
  ): List[Data] = {
    val filterFn = FilterFunction(
      name = "filter_f",
      filter = VeFilter(
        data = veAllocator.makeCVectors,
        condition = condition,
        stringVectorComputations = Nil
      ),
      onVe = false
    )

    import OriginalCallingContext.Automatic._
    evalFunction(filterFn.render.asInstanceOf[CFunction], "filter_f")(
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
  ): List[Data] = {
    val functionName = "sort_f"

    import OriginalCallingContext.Automatic._
    val cFunction =
      renderSort(sort =
        VeSort(
          data = veAllocator.makeCVectors.map(_.asInstanceOf[CScalarVector]),
          sorts = sorts.toList
        )
      )
    evalFunction(cFunction, functionName)(input = input.toList, veRetriever.makeCVectors)
  }

}
