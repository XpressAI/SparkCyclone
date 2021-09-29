package com.nec.spark.agile

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.agile.CFunctionGeneration.CExpression
import com.nec.spark.agile.SparkVeMapper.EvalFallback
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, Literal}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec

final class SparkVeMapperTest extends AnyFreeSpec {
  "Coalesce returns a non-nullable 'x', as we don't know if it's possible for it to be null" in {
    val res = SparkVeMapper.eval(
      Coalesce(Seq(AttributeReference.apply(name = "x", dataType = DoubleType, nullable = true)())),
      EvalFallback.noOp
    )
    expect(res == CExpression(cCode = "x", isNotNullCode = None))
  }

  "Coalesce of a nullable attribute and 0.0 gives a value, always" in {
    val res = SparkVeMapper.eval(
      Coalesce(
        Seq(AttributeReference("output_1_sum_nullable", DoubleType)(), Literal(0.0, DoubleType))
      ),
      EvalFallback.noOp
    )

    expect(res == CExpression("(output_1_sum_nullable_is_set) ? output_1_sum_nullable : 0.0", None))
  }

}
