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

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression.EvalFallback
import com.nec.ve._
import org.scalatest.freespec.AnyFreeSpec

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
@VectorEngineTest
final class ProjectExpressionEvalSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {

  import RealExpressionEvaluationUtils._

  "We can transform a column" in {
    expect(
      evalProject[Double, (Double, Double)](List[Double](90.0, 1.0, 2, 19, 14))(
        CExpression("2 * input_0->data[i]", None),
        CExpression("2 + input_0->data[i]", None)
      ) == List[(Double, Double)]((180, 92), (2, 3), (4, 4), (38, 21), (28, 16))
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

}
