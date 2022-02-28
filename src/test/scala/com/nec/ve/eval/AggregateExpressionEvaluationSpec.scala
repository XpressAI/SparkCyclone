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

import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration.JoinExpression.JoinProjection
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{veNullableDouble, VeNullableDouble}
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression.EvalFallback
import com.nec.spark.agile.{DeclarativeAggregationConverter, StringProducer}
import com.nec.ve._
import com.nec.ve.eval.StaticTypingTestAdditions._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{Corr, Sum}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.Ignore
import org.scalatest.freespec.AnyFreeSpec

@Ignore
final class AggregateExpressionEvaluationSpec
  extends AnyFreeSpec
  with WithVeProcess
  with VeKernelInfra {

  private implicit val fallback: EvalFallback = EvalFallback.noOp

  import RealExpressionEvaluationUtils._

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
