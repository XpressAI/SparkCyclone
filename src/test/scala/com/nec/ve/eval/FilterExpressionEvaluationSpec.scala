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
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression.EvalFallback
import com.nec.ve._
import org.scalatest.freespec.AnyFreeSpec

/**
 * This test suite evaluates expressions and Ve logical plans to verify correctness of the key bits.
 */
final class FilterExpressionEvaluationSpec
  extends AnyFreeSpec
  with WithVeProcess
  with VeKernelInfra {

  private implicit val fallback: EvalFallback = EvalFallback.noOp

  import RealExpressionEvaluationUtils._

  "Filter" ignore {

    "We can transform a null-column (FilterNull)" in {
      expect(
        evalFilter[Option[Double]](Some(90), None, Some(123))(
          CExpression(cCode = "input_0->data[i] != 90", isNotNullCode = None)
        ) == List[Option[Double]](None, Some(123))
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
            """std::string(input_0->data, input_0->offsets[i], input_0->offsets[i] + input_0->lengths[i]) == std::string("one")""",
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
  }

}
