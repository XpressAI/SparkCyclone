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
package com.nec.spark.agile

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.agile.StringHole.StringHoleEvaluation.SlowEvaluator
import com.nec.spark.agile.StringHole.{StringHoleEvaluation, StringHoleTransformation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CaseWhen, Literal, StartsWith}
import org.apache.spark.sql.types.StringType
import org.scalatest.freespec.AnyFreeSpec

final class StringHoleSpec extends AnyFreeSpec {
  "It detects a StartsWith in CASE WHEN" in {

    val aref = AttributeReference("test", StringType, nullable = false)()

    val y: Option[StringHoleTransformation] =
      StringHole.process(CaseWhen(Seq(StartsWith(aref, Literal("x")) -> Literal(1)), None))

    val x = y.get

    val evaluation =
      StringHoleEvaluation.SlowEvaluation("test", SlowEvaluator.StartsWithEvaluator("x"))
    expect(
      x.newExpression == CaseWhen(
        Seq(StringHole(StartsWith(aref, Literal("x")), evaluation) -> Literal(1)),
        None
      ),
      x.stringParts == List(evaluation)
    )

  }
}
