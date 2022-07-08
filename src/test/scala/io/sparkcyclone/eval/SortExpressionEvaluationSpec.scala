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
package io.sparkcyclone.eval

import com.eed3si9n.expecty.Expecty.expect
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.native.compiler.VeKernelInfra
import io.sparkcyclone.spark.codegen.CFunctionGeneration._
import io.sparkcyclone.spark.codegen.core._
import io.sparkcyclone.spark.codegen.SparkExpressionToCExpression.EvalFallback
import io.sparkcyclone.spark.codegen.sort.VeSortExpression
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, NullsFirst, NullsLast}
import io.sparkcyclone.rdd._
import io.sparkcyclone.vectorengine.WithVeProcess
import org.scalatest.freespec.AnyFreeSpec

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
@VectorEngineTest
final class SortExpressionEvaluationSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {

  import RealExpressionEvaluationUtils._

  "We can sort" in {
    expect(
      evalSort[(Double, Double)]((90.0, 5.0), (1.0, 4.0), (2.0, 2.0), (19.0, 1.0), (14.0, 3.0))(
        VeSortExpression(
          TypedCExpression2(
            VeNullableDouble,
            CExpression(cCode = "input_1->data[i]", isNotNullCode = None)
          ),
          Ascending,
          NullsLast
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
            VeNullableDouble,
            CExpression(cCode = "input_2->data[i]", isNotNullCode = None)
          ),
          Ascending,
          NullsLast
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
            VeNullableDouble,
            CExpression(cCode = "input_2->data[i]", isNotNullCode = None)
          ),
          Descending,
          NullsLast
        )
      )
    val expected =
      List[(Double, Double, Double)]((1.0, 4.0, 3.0), (90.0, 5.0, 1.0), (2.0, 2.0, 0.0))
    expect(results == expected)
  }

}
