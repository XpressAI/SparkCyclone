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

import com.nec.spark.agile.CFunctionGeneration.JoinExpression.JoinProjection
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.ve._
import com.nec.ve.eval.StaticTypingTestAdditions._
import org.scalatest.Ignore
import org.scalatest.freespec.AnyFreeSpec

@Ignore
final class JoinExpressionEvaluationSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {

  import RealExpressionEvaluationUtils._
  "We can Inner Join" in {
    val leftKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_0->data[i]", None))

    val rightKey =
      TypedCExpression2(VeScalarType.VeNullableDouble, CExpression("input_3->data[i]", None))

    val outputs = (
      TypedJoinExpression[Double](JoinProjection(CExpression("input_1->data[left_out[i]]", None))),
      TypedJoinExpression[Double](JoinProjection(CExpression("input_2->data[right_out[i]]", None))),
      TypedJoinExpression[Double](JoinProjection(CExpression("input_0->data[left_out[i]]", None))),
      TypedJoinExpression[Double](JoinProjection(CExpression("input_3->data[right_out[i]]", None)))
    )

    import JoinExpressor.RichJoin

    val out = evalInnerJoin[(Double, Double, Double, Double), (Double, Double, Double, Double)](
      List(
        (1.0, 2.0, 5.0, 1.0),
        (3.0, 2.0, 3.0, 7.0),
        (11.0, 7.0, 12.0, 11.0),
        (8.0, 2.0, 3.0, 9.0)
      ),
      leftKey,
      rightKey,
      outputs.expressed
    )

    assert(out == List((2.0, 5.0, 1.0, 1.0), (7.0, 12.0, 11.0, 11.0)))
  }

}
