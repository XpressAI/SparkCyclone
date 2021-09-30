package com.nec.cmake.eval

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CExpression}
import com.nec.spark.agile.DeclarativeAggregationConverter
import com.nec.spark.agile.SparkVeMapper.EvalFallback
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, Sum}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal, Multiply}
import org.apache.spark.sql.types.IntegerType
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
        .cCode == "((2) * (test_sum_nullable))"
    )
  }
}
