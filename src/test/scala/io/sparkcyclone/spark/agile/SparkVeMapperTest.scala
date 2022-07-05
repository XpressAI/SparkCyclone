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
package io.sparkcyclone.spark.agile

import com.eed3si9n.expecty.Expecty.expect
import io.sparkcyclone.spark.agile.CFunctionGeneration.CExpression
import io.sparkcyclone.spark.agile.SparkExpressionToCExpression.EvalFallback
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, Literal}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec
import io.sparkcyclone.spark.agile.SparkExpressionToCExpression.EvaluationAttempt._

final class SparkVeMapperTest extends AnyFreeSpec {

  private implicit val fb = EvalFallback.noOp

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
