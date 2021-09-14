package com.nec.cmake.eval

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CExpression}
import com.nec.spark.agile.DeclarativeAggregationConverter
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types.DoubleType
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

  "Declarative-aggregate based" - {
    val res = DeclarativeAggregationConverter(
      Sum(AttributeReference("input_0->data[i]", DoubleType, nullable = false)())
    )

    "initial is computed" in {
      assert(res.initial("x") == CodeLines.from("double x_0_sum = 0;"))
    }
    "compute is computed" in {
      assert(res.compute("x") == CodeLines.empty)
    }
    "iterate is computed" in {
      assert(res.iterate("x") == CodeLines.from("x_0_sum = x_0_sum + input_0->data[i];"))
    }
    "fetch is computed" in {
      assert(res.fetch("x") == CExpression("x_0_sum", None))
    }
    "free is computed" in {
      assert(res.free("x") == CodeLines.empty)
    }
  }
}
