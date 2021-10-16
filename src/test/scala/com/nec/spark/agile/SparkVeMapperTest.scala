package com.nec.spark.agile

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.agile.CFunctionGeneration.CExpression
import com.nec.spark.agile.SparkExpressionToCExpression.EvalFallback
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, Literal}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.agile.SparkExpressionToCExpression.EvaluationAttempt._

final class SparkVeMapperTest extends AnyFreeSpec {
  private implicit val fb = EvalFallback.noOp
  "Coalesce returns a non-nullable 'x', as we don't know if it's possible for it to be null" in {
    val res = SparkExpressionToCExpression
      .eval(
        Coalesce(
          Seq(AttributeReference.apply(name = "x", dataType = DoubleType, nullable = true)())
        )
      )
      .getOrReport()
    expect(res == CExpression(cCode = "x", isNotNullCode = None))
  }

  "Coalesce of a nullable attribute and 0.0 gives a value, always" in {
    val res = SparkExpressionToCExpression
      .eval(
        Coalesce(
          Seq(AttributeReference("output_1_sum_nullable", DoubleType)(), Literal(0.0, DoubleType))
        )
      )
      .getOrReport()

    expect(res == CExpression("(output_1_sum_nullable_is_set) ? output_1_sum_nullable : 0.0", None))
  }

}
