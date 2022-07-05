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
package io.sparkcyclone.ve.eval

import io.sparkcyclone.spark.agile.core.CodeLines
import io.sparkcyclone.spark.agile.CFunctionGeneration.{Aggregation, CExpression}
import io.sparkcyclone.spark.agile.DeclarativeAggregationConverter
import io.sparkcyclone.spark.agile.SparkExpressionToCExpression.EvalFallback
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  Average,
  Final,
  Sum
}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal, Multiply}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.scalatest.freespec.AnyFreeSpec

object SparkToVeAggregatorSpec {}
final class SparkToVeAggregatorSpec extends AnyFreeSpec {
  val as = Aggregation.sum(CExpression("abc", None))
  "sum works" - {
    "initial is computed" in {
      assert(as.initial("x") == CodeLines.from("double x_aggregate_sum = 0;"))
    }
    "compute is computed" in {
      assert(as.compute("x") == CodeLines.empty)
    }
    "iterate is computed" in {
      assert(as.iterate("x") == CodeLines.from("x_aggregate_sum += abc;"))
    }
    "fetch is computed" in {
      assert(as.fetch("x") == CExpression("x_aggregate_sum", None))
    }
    "free is computed" in {
      assert(as.free("x") == CodeLines.empty)
    }
  }
  private implicit val fb = EvalFallback.noOp

  "We can do a simple replacement with an Aggregation" in {
    val inputQuery = Multiply(
      Literal(2, IntegerType),
      AggregateExpression(Sum(AttributeReference("x", IntegerType)()), Final, isDistinct = false)
    )

    assert(
      DeclarativeAggregationConverter
        .transformingFetch(inputQuery)
        .getOrElse(fail("Not found"))
        .fetch("test")
        .cCode == "((2) * (test_0_attr_sum_nullable))"
    )
  }

  "We can do a more complex replacement with an Aggregation" in {
    val inputQuery = Multiply(
      AggregateExpression(
        Average(AttributeReference("y", IntegerType)()),
        Final,
        isDistinct = false
      ),
      AggregateExpression(Sum(AttributeReference("x", IntegerType)()), Final, isDistinct = false)
    )

    val result =
      DeclarativeAggregationConverter
        .transformingFetch(inputQuery)
        .getOrElse(fail("Not found"))
        .fetch("test")
        .cCode

    assert(
      result == "((((test_0_attr_sum_nullable) / ((double) (test_0_attr_count_nullable)))) * (test_1_attr_sum_nullable))"
    )
  }

  "Merging 2 attributes" in {
    assert(
      DeclarativeAggregationConverter
        .rewriteMerge("output", "input")(Average(AttributeReference("x", DoubleType)())) ==
        List(
          CExpression(
            "((output_attr_sum_nullable) + (input_attr_sum->data[i]))",
            Some(
              "(output_attr_sum_nullable_is_set && input_attr_sum->get_validity(i))"
            )
          ),
          CExpression(
            "((output_attr_count_nullable) + (input_attr_count->data[i]))",
            Some(
              "(output_attr_count_nullable_is_set && input_attr_count->get_validity(i))"
            )
          )
        )
    )
  }
}
