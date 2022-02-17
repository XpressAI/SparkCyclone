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
package com.nec.cmake.eval

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.InputArrowVectorWrapper
import com.nec.arrow.ArrowVectorBuilders.{
  withArrowStringVector,
  withDirectBigIntVector,
  withDirectFloat8Vector,
  withNullableArrowStringVector
}
import com.nec.arrow.{CArrowNativeInterface, CatsArrowVectorBuilders, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.eval.StaticTypingTestAdditions._
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.ve.FilterFunction
import com.nec.spark.agile.CFunctionGeneration.JoinExpression.JoinProjection
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression.EvalFallback
import com.nec.spark.agile.join.GenericJoiner.{FilteredOutput, Join}
import com.nec.spark.agile.join.{GenericJoiner, JoinByEquality}
import com.nec.spark.agile.{CFunctionGeneration, DeclarativeAggregationConverter, StringProducer}
import com.nec.util.RichVectors.{RichBigIntVector, RichFloat8, RichIntVector, RichVarCharVector}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{Corr, Sum}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
final class RealExpressionEvaluationSpec extends AnyFreeSpec {
  import com.nec.cmake.eval.RealExpressionEvaluationSpec._
  private implicit val fallback: EvalFallback = EvalFallback.noOp

  "We can transform a column" in {
    expect(
      evalProject(List[Double](90.0, 1.0, 2, 19, 14))(
        TypedCExpression[Double](CExpression("2 * input_0->data[i]", None)),
        TypedCExpression[Double](CExpression("2 + input_0->data[i]", None))
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

  /*
  "We can transform a column to a String and a Double" in {

    def expr_to_string(cExpression: CExpression): ImperativeStringProducer =
      (tsn, iln) =>
        CodeLines
          .from(
            s"std::string len_str = std::to_string(${cExpression.cCode});",
            s"${tsn}.append(len_str);",
            s"${iln} += len_str.size();"
          )
    assert(
      evalProject(List[Double](90.0, 1.0, 2, 19, 14))(
        expr_to_string(CExpression("2 * input_0->data[i]", None)): StringProducer,
        TypedCExpression[Double](CExpression("2 + input_0->data[i]", None))
      ) == List[(String, Double)](
        ("180.000000", 92.0),
        ("2.000000", 3.0),
        ("4.000000", 4.0),
        ("38.000000", 21.0),
        ("28.000000", 16.0)
      )
    )
  }
   */

  "We can project a null-column (ProjectNull)" in {
    expect(
      evalProject(List[Double](90.0, 1.0, 2, 19, 14))(
        TypedCExpression[Double](CExpression("2 * input_0->data[i]", None)),
        TypedCExpression[Option[Double]](
          CExpression("2 + input_0->data[i]", Some("input_0->data[i] == 2"))
        )
      ) == List[(Double, Option[Double])](
        (180, None),
        (2, None),
        (4, Some(4)),
        (38, None),
        (28, None)
      )
    )
  }

  /*
  "We can transform a String column to a Double" in {
    expect(
      evalProject(List[String]("90.0", "1.0", "2", "19", "14"))(
        TypedCExpression[Double](
          CExpression(
            "2 + atof(std::string(input_0->data, input_0->offsets[i], input_0->offsets[i+1] - input_0->offsets[i]).c_str())",
            None
          )
        )
      ) == List[Double](92.0, 3.0, 4.0, 21.0, 16.0)
    )
  }
   */

  "We can transform a null-column" in {
    expect(
      evalProject(List[Double](90.0, 1.0, 2, 19, 14))(
        TypedCExpression[Double](CExpression("2 * input_0->data[i]", None)),
        TypedCExpression[Option[Double]](CExpression("2 + input_0->data[i]", Some("0")))
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
          """std::string(input_0->data, input_0->offsets[i], input_0->offsets[i+1] - input_0->offsets[i]) == std::string("one")""",
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
    val result = evalAggregate(
      List[(Double, Double, Double)]((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0))
    )(
      TypedGroupByExpression[Double](
        GroupByAggregation(
          Aggregation.sum(CExpression("input_2->data[i] - input_0->data[i]", None))
        )
      )
    )
    assert(result == List[Double](6.6))
  }

  "Average is computed correctly" in {
    val result = evalAggregate(List[Double](1, 2, 3))(
      TypedGroupByExpression[Double](
        GroupByAggregation(Aggregation.avg(CExpression("input_0->data[i]", None)))
      )
    )
    assert(result == List[Double](2))
  }

  "We can aggregate / group by (simple sum)" in {
    val result = evalGroupBySum(
      List[(Double, Double, Double)]((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0), (3, 4, 9))
    )(
      (
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None)),
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
      )
    )(
      (
        TypedGroupByExpression[Double](GroupByProjection(CExpression("input_0->data[i]", None))),
        TypedGroupByExpression[Double](
          GroupByProjection(CExpression("input_1->data[i] + 1", None))
        ),
        TypedGroupByExpression[Double](
          GroupByAggregation(
            Aggregation.sum(CExpression("input_2->data[i] - input_0->data[i]", None))
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
      evalGroupBySumStr(input)(
        (
          StringGrouping("input_0"),
          TypedCExpression2(
            VeScalarType.veNullableDouble,
            CExpression("input_1->data[i] * input_1->data[i]", None)
          )
        )
      )(
        (
          StringGrouping("input_0"),
          TypedGroupByExpression[Double](
            GroupByAggregation(Aggregation.sum(CExpression("input_1->data[i]", None)))
          )
        )
      )

    assert(result.asInstanceOf[List[(String, Double)]].sorted == expected.sorted)
  }

  "We can aggregate / group by with NULL input check values" in {
    val result = evalGroupBySum(
      List[(Double, Double, Double)]((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0))
    )(
      (
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None)),
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
      )
    )(
      (
        TypedGroupByExpression[Double](GroupByProjection(CExpression("input_0->data[i]", None))),
        TypedGroupByExpression[Double](
          GroupByProjection(CExpression("input_1->data[i] + 1", None))
        ),
        TypedGroupByExpression[Double](
          GroupByAggregation(
            Aggregation.sum(
              CExpression("input_2->data[i] - input_0->data[i]", Some("input_2->data[i] != 4.0"))
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
    val result = evalGroupBySum(
      List[(Double, Double, Double)]((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0))
    )(
      (
        TypedCExpression2(
          VeScalarType.veNullableDouble,
          CExpression("input_0->data[i]", Some("input_2->data[i] != 4.0"))
        ),
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
      )
    )(
      (
        TypedGroupByExpression[Option[Double]](
          GroupByProjection(CExpression("input_0->data[i]", Some("input_2->data[i] != 4.0")))
        ),
        TypedGroupByExpression[Double](
          GroupByProjection(CExpression("input_1->data[i] + 1", None))
        ),
        TypedGroupByExpression[Double](
          GroupByAggregation(
            Aggregation.sum(CExpression("input_2->data[i] - input_0->data[i]", None))
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
    val result = evalGroupBySum(
      List[(Option[Double], Double, Double)](
        (Some(1.0), 2.0, 3.0),
        (Some(1.5), 1.2, 3.1),
        (None, 2.0, 4.0)
      )
    )(
      (
        TypedCExpression2(
          VeScalarType.veNullableDouble,
          CExpression("input_0->data[i]", Some("input_0->get_validity(i)"))
        ),
        TypedCExpression2(
          VeScalarType.veNullableDouble,
          CExpression("input_1->data[i]", Some("input_1->get_validity(i)"))
        )
      )
    )(
      (
        TypedGroupByExpression[Option[Double]](
          GroupByProjection(
            CExpression("input_0->data[i]", Some("input_0->get_validity(i)"))
          )
        ),
        TypedGroupByExpression[Double](
          GroupByProjection(
            CExpression("input_1->data[i] + 1", Some("input_1->get_validity(i)"))
          )
        ),
        TypedGroupByExpression[Option[Double]](
          GroupByAggregation(
            Aggregation.sum(
              CExpression(
                "input_2->data[i] - input_0->data[i]",
                Some(
                  "input_0->get_validity(i) && input_2->get_validity(i)"
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
    val result = evalGroupBySum(
      List[(Double, Double, Double)]((1.0, 2.0, 3.0), (1.5, 1.2, 3.1), (1.0, 2.0, 4.0))
    )(
      (
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None)),
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
      )
    )(
      (
        TypedGroupByExpression[Double](GroupByProjection(CExpression("input_0->data[i]", None))),
        TypedGroupByExpression[Double](
          GroupByProjection(CExpression("input_1->data[i] + 1", None))
        ),
        TypedGroupByExpression[Double](
          GroupByAggregation(
            DeclarativeAggregationConverter(
              Sum(AttributeReference("input_0->data[i]", DoubleType)())
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
    val inputs = List(
      (1.0, 2.0, 5.0, 1.0),
      (3.0, 2.0, 3.0, 7.0),
      (11.0, 7.0, 12.0, 11.0),
      (8.0, 2.0, 3.0, 9.0)
    )
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

    val out = evalInnerJoin(inputs, leftKey, rightKey, outputs)

    assert(out == List((2.0, 5.0, 1.0, 1.0), (7.0, 12.0, 11.0, 11.0)))
  }

  "We can Left Join" in {
    val inputs = List(
      (1.0, 2.0, 5.0, 1.0),
      (3.0, 2.0, 3.0, 7.0),
      (11.0, 7.0, 12.0, 11.0),
      (8.0, 2.0, 3.0, 9.0)
    )
    val leftKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_0->data[i]", None))

    val rightKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_3->data[i]", None))

    val innerOutputs = (
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_1->data[left_out[i]]", None))
      ),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_2->data[right_out[i]]", None))
      ),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_0->data[left_out[i]]", None))
      ),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_3->data[right_out[i]]", None))
      )
    )

    val outerOutputs = (
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_1->data[outer_idx[idx]]", None))
      ),
      TypedJoinExpression[Option[Double]](JoinProjection(CExpression("0", Some("false")))),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_0->data[outer_idx[idx]]", None))
      ),
      TypedJoinExpression[Option[Double]](JoinProjection(CExpression("0", Some("false"))))
    )

    val out = evalOuterJoin(inputs, leftKey, rightKey, innerOutputs, outerOutputs, LeftOuterJoin)

    assert(
      out == List(
        (Some(2.0), Some(5.0), Some(1.0), Some(1.0)),
        (Some(7.0), Some(12.0), Some(11.0), Some(11.0)),
        (Some(2.0), None, Some(3.0), None),
        (Some(2.0), None, Some(8.0), None)
      )
    )
  }

  "We can Right Join" in {
    val inputs = List(
      (1.0, 2.0, 5.0, 1.0),
      (3.0, 2.0, 3.0, 7.0),
      (11.0, 7.0, 12.0, 11.0),
      (8.0, 2.0, 3.0, 9.0)
    )
    val leftKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_0->data[i]", None))

    val rightKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_3->data[i]", None))

    val innerOutputs = (
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_1->data[left_out[i]]", None))
      ),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_2->data[right_out[i]]", None))
      ),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_0->data[left_out[i]]", None))
      ),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_3->data[right_out[i]]", None))
      )
    )

    val outerOutputs = (
      TypedJoinExpression[Option[Double]](JoinProjection(CExpression("0", Some("false")))),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_2->data[outer_idx[idx]]", None))
      ),
      TypedJoinExpression[Option[Double]](JoinProjection(CExpression("0", Some("false")))),
      TypedJoinExpression[Option[Double]](
        JoinProjection(CExpression("input_3->data[outer_idx[idx]]", None))
      )
    )

    val out = evalOuterJoin(inputs, leftKey, rightKey, innerOutputs, outerOutputs, RightOuterJoin)

    assert(
      out == List(
        (Some(2.0), Some(5.0), Some(1.0), Some(1.0)),
        (Some(7.0), Some(12.0), Some(11.0), Some(11.0)),
        (None, Some(3.0), None, Some(7.0)),
        (None, Some(3.0), None, Some(9.0))
      )
    )
  }

  "We can aggregate / group by (correlation)" in {
    val result = evalGroupBySum(
      List[(Double, Double, Double)](
        (1.0, 2.0, 3.0),
        (1.5, 1.2, 3.1),
        (1.0, 2.0, 4.0),
        (1.5, 1.2, 4.1)
      )
    )(
      (
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_0->data[i]", None)),
        TypedCExpression2(VeScalarType.veNullableDouble, CExpression("input_1->data[i]", None))
      )
    )(
      (
        TypedGroupByExpression[Double](GroupByProjection(CExpression("input_0->data[i]", None))),
        TypedGroupByExpression[Double](
          GroupByProjection(CExpression("input_1->data[i] + 1", None))
        ),
        TypedGroupByExpression[Double](
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
    assert(
      result ==
        List[(Double, Double, Double)]((1.0, 3.0, 1.0), (1.5, 2.2, 1.0))
    )
  }

  "Join" - {

    val left = List[(String, Long, Int)](
      ("foo", 42, 43),
      ("test", 123, 456),
      ("test2", 123, 4567),
      ("test2", 12, 45678),
      ("test3", 12, 456789),
      ("test3", 123, 4567890)
    )

    val right = List[(String, Long, Double)](
      ("foo", 42, 43),
      ("test2", 123, 654),
      ("test2", 123, 761),
      ("test3", 12, 456),
      ("bar", 0, 0)
    )

    "We can get indices of join (JoinOnlyIndices)" in {
      val inputsLeft =
        List(CVector.varChar("x_a"), CVector.bigInt("x_b"), CVector.int("x_c"))
      val inputsRight =
        List(CVector.varChar("y_a"), CVector.bigInt("y_b"), CVector.double("y_c"))
      val firstJoin = Join(left = inputsLeft(0), right = inputsRight(0))
      val secondJoin = Join(left = inputsLeft(1), right = inputsRight(1))

      val evaluationResource = for {
        allocator <- WithTestAllocator.resource
        vb = CatsArrowVectorBuilders(cats.effect.Ref.unsafe[IO, Int](0))(allocator)

        x_a <- vb.stringVector(left.map(_._1))
        x_b <- vb.longVector(left.map(_._2))
        x_c <- vb.intVector(left.map(_._3))

        y_a <- vb.stringVector(right.map(_._1))
        y_b <- vb.longVector(right.map(_._2))
        y_c <- vb.doubleVector(right.map(_._3))

        idx_left <- vb.intVector(Seq.empty)
        idx_right <- vb.intVector(Seq.empty)

        cLib <- Resource.eval {
          IO.delay {
            CMakeBuilder.buildCLogging(
              List(
                "\n\n",
                GenericJoiner.printVec.cCode, {
                  val inputsLeft =
                    List(CVector.varChar("x_a"), CVector.bigInt("x_b"), CVector.int("x_c"))
                  val inputsRight =
                    List(CVector.varChar("y_a"), CVector.bigInt("y_b"), CVector.double("y_c"))
                  val firstJoin = Join(left = inputsLeft(0), right = inputsRight(0))
                  val secondJoin = Join(left = inputsLeft(1), right = inputsRight(1))
                  JoinByEquality(
                    inputsLeft = inputsLeft,
                    inputsRight = inputsRight,
                    joins = List(firstJoin, secondJoin)
                  ).produceIndices.toCodeLinesS("adv_join").cCode
                }
              )
                .mkString("\n\n")
            )
          }
        }

        nativeInterface = new CArrowNativeInterface(cLib.toString)
        _ <- Resource.eval {
          IO.delay {
            nativeInterface.callFunctionWrapped(
              name = "adv_join",
              arguments = List(
                NativeArgument.input(x_a),
                NativeArgument.input(x_b),
                NativeArgument.input(x_c),
                NativeArgument.input(y_a),
                NativeArgument.input(y_b),
                NativeArgument.input(y_c),
                NativeArgument.output(idx_left),
                NativeArgument.output(idx_right)
              )
            )
          }
        }
      } yield (idx_left.toList, idx_right.toList)

      val evaluation = evaluationResource.use { case (output_idx_left, output_idx_right) =>
        IO.delay {
          expect(output_idx_left == List(0, 2, 2, 4), output_idx_right == List(0, 1, 2, 3))
        }
      }

      evaluation.unsafeRunSync()

    }

    "We can join by String & Long (JoinByString)" in {

      /**
       * SELECT X.A, X.C, Y.C FROM X LEFT JOIN Y ON X.A = Y.A AND X.B = Y.B
       * X = [A: String, B: Long, C: Int]
       * Y = [A: String, B: Long, C: Double]
       */

      val joinSideBySide = List[((String, Long, Int), (String, Long, Double))](
        /** two inner join entries on RHS */
        (("foo", 42, 43), ("foo", 42, 43)),
        (("test2", 123, 4567), ("test2", 123, 654)),
        (("test2", 123, 4567), ("test2", 123, 761)),
        (("test3", 12, 456789), ("test3", 12, 456))
      )

      val joinSelectOnlyIntDouble = List[(String, Int, Double)](
        ("foo", 43, 43),
        ("test2", 4567, 654),
        ("test2", 4567, 761),
        ("test3", 456789, 456)
      )

      val evaluationResource = for {
        allocator <- WithTestAllocator.resource
        vb = CatsArrowVectorBuilders(cats.effect.Ref.unsafe[IO, Int](0))(allocator)
        x_a <- vb.stringVector(left.map(_._1))
        x_b <- vb.longVector(left.map(_._2))
        x_c <- vb.intVector(left.map(_._3))
        y_a <- vb.stringVector(right.map(_._1))
        y_b <- vb.longVector(right.map(_._2))
        y_c <- vb.doubleVector(right.map(_._3))
        o_a <- vb.stringVector(Seq.empty)
        o_b <- vb.intVector(Seq.empty)
        o_c <- vb.doubleVector(Seq.empty)

        cLib <- Resource.eval {
          IO.delay {
            CMakeBuilder.buildCLogging(
              List(
                "\n\n",
                GenericJoiner.printVec.cCode, {
                  val inputsLeft =
                    List(CVector.varChar("x_a"), CVector.bigInt("x_b"), CVector.int("x_c"))
                  val inputsRight =
                    List(CVector.varChar("y_a"), CVector.bigInt("y_b"), CVector.double("y_c"))
                  val firstJoin = Join(left = inputsLeft(0), right = inputsRight(0))
                  val secondJoin = Join(left = inputsLeft(1), right = inputsRight(1))
                  val genericJoiner = GenericJoiner(
                    inputsLeft = inputsLeft,
                    inputsRight = inputsRight,
                    joins = List(firstJoin, secondJoin),
                    outputs = List(
                      FilteredOutput("o_a", inputsLeft(0)),
                      FilteredOutput("o_b", inputsLeft(2)),
                      FilteredOutput("o_c", inputsRight(2))
                    )
                  )
                  val functionName = "adv_join"
                  val produceIndicesFName = s"indices_${functionName}"
                  CodeLines
                    .from(
                      CFunctionGeneration.KeyHeaders,
                      genericJoiner.cFunctionExtra.toCodeLinesNoHeader(produceIndicesFName),
                      genericJoiner
                        .cFunction(produceIndicesFName)
                        .toCodeLinesNoHeader(functionName)
                    )
                    .cCode
                }
              )
                .mkString("\n\n")
            )
          }
        }

        nativeInterface = new CArrowNativeInterface(cLib.toString)
        _ <- Resource.eval {
          IO.delay {
            nativeInterface.callFunctionWrapped(
              name = "adv_join",
              arguments = List(
                NativeArgument.input(x_a),
                NativeArgument.input(x_b),
                NativeArgument.input(x_c),
                NativeArgument.input(y_a),
                NativeArgument.input(y_b),
                NativeArgument.input(y_c),
                NativeArgument.output(o_a),
                NativeArgument.output(o_b),
                NativeArgument.output(o_c)
              )
            )
          }
        }
      } yield (o_a.toList, o_b.toList, o_c.toList)

      val evaluation = evaluationResource.use { case (output_a, output_b, output_c) =>
        IO.delay {
          val expected_a = joinSelectOnlyIntDouble.map(_._1)
          val expected_b = joinSelectOnlyIntDouble.map(_._2)
          val expected_c = joinSelectOnlyIntDouble.map(_._3)
          expect(output_a == expected_a, output_b == expected_b, output_c == expected_c)
        }
      }

      evaluation.unsafeRunSync()

    }
  }

}

object RealExpressionEvaluationSpec extends LazyLogging {

  def evalAggregate[Input, Output](input: List[Input])(expressions: Output)(implicit
    inputArguments: InputArgumentsScalar[Input],
    groupExpressor: GroupExpressor[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "agg"

    val generatedSource =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = inputArguments.inputs,
          groups = Nil,
          outputs = groupExpressor.express(expressions).map(v => Right(v))
        )
      ).renderGroupBy.toCodeLinesG(functionName)

    logger.debug(s"Generated code: ${generatedSource.cCode}")

    val cLib = CMakeBuilder.buildCLogging(
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalInnerJoin[Input, LeftKey, RightKey, Output](
    input: List[Input],
    leftKey: TypedCExpression2,
    rightKey: TypedCExpression2,
    output: Output
  )(implicit
    inputArguments: InputArgumentsScalar[Input],
    joinExpressor: JoinExpressor[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"
    val generatedSource =
      renderInnerJoin(
        VeInnerJoin(
          inputs = inputArguments.inputs,
          leftKey = leftKey,
          rightKey = rightKey,
          outputs = joinExpressor.express(output)
        )
      ).toCodeLinesS(functionName)

    logger.debug(s"Generated code: ${generatedSource.cCode}")

    val cLib = CMakeBuilder.buildCLogging(
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalOuterJoin[Input, LeftKey, RightKey, Output](
    input: List[Input],
    leftKey: TypedCExpression2,
    rightKey: TypedCExpression2,
    innerOutput: Output,
    outerOutput: Output,
    joinType: JoinType
  )(implicit
    inputArguments: InputArgumentsScalar[Input],
    joinExpressor: JoinExpressor[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"
    val outputs = joinExpressor
      .express(innerOutput)
      .zip(joinExpressor.express(outerOutput))
      .map { case (inner, outer) =>
        OuterJoinOutput(inner, outer)
      }
    val generatedSource =
      renderOuterJoin(
        VeOuterJoin(
          inputs = inputArguments.inputs,
          leftKey = leftKey,
          rightKey = rightKey,
          outputs = outputs,
          joinType
        )
      ).toCodeLinesS(functionName)
    logger.debug(s"Generated code: ${generatedSource.cCode}")

    val cLib = CMakeBuilder.buildCLogging(
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalGroupBySum[Input, Groups, Output](
    input: List[Input]
  )(groups: (TypedCExpression2, TypedCExpression2))(expressions: Output)(implicit
    inputArguments: InputArgumentsScalar[Input],
    groupExpressor: GroupExpressor[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"

    val generatedSource =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = inputArguments.inputs,
          groups = List(Right(groups._1), Right(groups._2)),
          outputs = groupExpressor.express(expressions).map(v => Right(v))
        )
      ).renderGroupBy.toCodeLinesG(functionName)

    logger.debug(s"Generated code: ${generatedSource.cCode}")

    val cLib = CMakeBuilder.buildCLogging(
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalGroupBySumStr[Input, Groups, Output](
    input: List[Input]
  )(groups: (StringGrouping, TypedCExpression2))(expressions: Output)(implicit
    inputArguments: InputArgumentsFull[Input],
    groupExpressor: GeneralGroupExpressor[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"

    val generatedSource =
      OldUnifiedGroupByFunctionGeneration(
        VeGroupBy(
          inputs = inputArguments.inputs,
          groups = List(Left(groups._1), Right(groups._2)),
          outputs = groupExpressor.express(expressions)
        )
      ).renderGroupBy.toCodeLinesG(functionName)

    logger.debug(s"Generated code: ${generatedSource.cCode}")

    val cLib = CMakeBuilder.buildCLogging(cSource =
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalProject[Input, Output](input: List[Input])(expressions: Output)(implicit
    inputArguments: InputArgumentsFull[Input],
    projectExpression: ProjectExpression[Output],
    outputArguments: OutputArguments[Output]
  ): List[outputArguments.Result] = {
    val functionName = "project_f"

    val generatedSource =
      renderProjection(
        VeProjection(
          inputs = inputArguments.inputs,
          outputs = projectExpression.outputs(expressions)
        )
      ).toCodeLinesPF(functionName)

    val cLib = CMakeBuilder.buildCLogging(
      List(generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalFilter[Data](input: Data*)(condition: CExpression)(implicit
    inputArguments: InputArgumentsFull[Data],
    outputArguments: OutputArguments[Data]
  ): List[outputArguments.Result] = {
    val filterFn = FilterFunction(
      "filter_f",
      VeFilter(
        data = inputArguments.inputs,
        condition = condition,
        stringVectorComputations = Nil
      ),
      false
    )

    val generatedSource = CodeLines.from(
      """#include "cyclone/cyclone.hpp"""",
      """#include "cyclone/transfer-definitions.hpp"""",
      filterFn.toCodeLines
    )

    val cLib = CMakeBuilder.buildCLogging(
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(filterFn.name, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  def evalSort[Data](input: Data*)(sorts: VeSortExpression*)(implicit
    inputArguments: InputArgumentsScalar[Data],
    outputArguments: OutputArguments[Data]
  ): List[outputArguments.Result] = {
    val functionName = "sort_f"

    val generatedSource =
      renderSort(sort = VeSort(data = inputArguments.inputs, sorts = sorts.toList))
        .toCodeLinesS(functionName)

    val cLib = CMakeBuilder.buildC(
      List("\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val (outArgs, fetcher) = outputArguments.allocateVectors()
      try {
        val inVecs = inputArguments.allocateVectors(input: _*)
        try nativeInterface.callFunctionWrapped(functionName, inVecs ++ outArgs)
        finally {
          inVecs
            .collect { case VectorInputNativeArgument(v: InputArrowVectorWrapper) =>
              v.valueVector
            }
            .foreach(_.close())
        }
        fetcher()
      } finally outArgs.foreach(_.wrapped.valueVector.close())
    }
  }

  final case class SplitGroupBy[Partial, Output](
    stringGrouping: StringGrouping,
    output: (StringGrouping, TypedGroupByExpression[Double])
  ) {

    def generator: OldUnifiedGroupByFunctionGeneration = OldUnifiedGroupByFunctionGeneration(
      VeGroupBy(
        inputs = List(CVector.varChar("input_0"), CVector.double("input_1")),
        groups = List(Left(stringGrouping)),
        outputs = List(
          Left(NamedStringProducer("output_0", StringProducer.copyString(output._1.name))),
          Right(
            NamedGroupByExpression(
              "output_1",
              VeScalarType.veNullableDouble,
              output._2.groupByExpression
            )
          )
        )
      )
    )

    def evalFull(inputData: List[(String, Double)]): List[(String, Double)] = evalFinal(
      evalPartial(inputData)
    )

    def evalFinal(partialInputData: List[(String, Double, Long)]): List[(String, Double)] = {
      val functionName = "aggregate_final"

      val generatedSource: CodeLines =
        ??? // = generator.renderFinalGroupBy.toCodeLines(functionName)

      logger.debug(s"Generated code: ${generatedSource.cCode}")

      val cLib = CMakeBuilder.buildCLogging(
        List(generatedSource.cCode)
          .mkString("\n\n")
      )

      val nativeInterface = new CArrowNativeInterface(cLib.toString)
      WithTestAllocator { implicit allocator =>
        withArrowStringVector(partialInputData.map(_._1)) { vcv =>
          withDirectFloat8Vector(partialInputData.map(_._2)) { f8v =>
            withDirectBigIntVector(partialInputData.map(_._3)) { iv =>
              withNullableArrowStringVector(Seq.empty) { vcv_out =>
                withDirectFloat8Vector(Seq.empty) { f8v_out =>
                  nativeInterface.callFunctionWrapped(
                    functionName,
                    List(
                      NativeArgument.input(vcv),
                      NativeArgument.input(f8v),
                      NativeArgument.input(iv),
                      NativeArgument.output(vcv_out),
                      NativeArgument.output(f8v_out)
                    )
                  )

                  vcv_out.toList.zip(f8v_out.toList)
                }
              }
            }
          }
        }
      }

    }

    def evalPartial(inputData: List[(String, Double)]): List[(String, Double, Long)] = {
      val functionName = "aggregate_partial"

      val generatedSource = CodeLines.empty //.renderPartialGroupBy.toCodeLines(functionName)

      logger.debug(s"Generated code: ${generatedSource.cCode}")

      val cLib = CMakeBuilder.buildCLogging(
        List("\n\n", generatedSource.cCode)
          .mkString("\n\n")
      )

      val nativeInterface = new CArrowNativeInterface(cLib.toString)
      WithTestAllocator { implicit allocator =>
        withArrowStringVector(inputData.map(_._1)) { vcv =>
          withDirectFloat8Vector(inputData.map(_._2)) { f8v =>
            withNullableArrowStringVector(Seq.empty) { vcv_out =>
              withDirectFloat8Vector(Seq.empty) { f8v_out =>
                withDirectBigIntVector(Seq.empty) { iv_out =>
                  nativeInterface.callFunctionWrapped(
                    functionName,
                    List(
                      NativeArgument.input(vcv),
                      NativeArgument.input(f8v),
                      NativeArgument.output(vcv_out),
                      NativeArgument.output(f8v_out),
                      NativeArgument.output(iv_out)
                    )
                  )

                  vcv_out.toList.zip(f8v_out.toList).zip(iv_out.toList).map { case ((s, d), i) =>
                    (s, d, i)
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
