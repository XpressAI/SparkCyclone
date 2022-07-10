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
package io.sparkcyclone.spark.codegen

import io.sparkcyclone.spark.codegen.CFunctionGeneration.CExpression
import org.scalatest.freespec.AnyFreeSpec

final class FlatToNestedFunctionTest extends AnyFreeSpec {
  "It works for 3" in {
    assert(
      FlatToNestedFunction.nest(Seq("a", "b", "c"), "MAX") == s"MAX(a, MAX(b, c))"
    )
  }

  "It works for 2" in {
    assert(FlatToNestedFunction.nest(Seq("a", "b"), "MAX") == s"MAX(a, b)")
  }

  "In case of 2 items, it checks their nulls" in {
    assert(
      FlatToNestedFunction.runWhenNotNull(
        items = List(
          CExpression(cCode = "a", isNotNullCode = Some("1")),
          CExpression(cCode = "b", isNotNullCode = Some("2"))
        ),
        function = "MAX"
      ) == CExpression(
        cCode = "(1 && 2) ? (MAX(a, b)) : (1 ? a : b)",
        isNotNullCode = Some("1 || 2")
      )
    )
  }
  "In case of 2 items, only 1 nullable" in {
    assert(
      FlatToNestedFunction.runWhenNotNull(
        items = List(
          CExpression(cCode = "a", isNotNullCode = None),
          CExpression(cCode = "b", isNotNullCode = Some("2"))
        ),
        function = "MAX"
      ) == CExpression(cCode = "(2) ? (MAX(a, b)) : a", isNotNullCode = None)
    )
  }
}
