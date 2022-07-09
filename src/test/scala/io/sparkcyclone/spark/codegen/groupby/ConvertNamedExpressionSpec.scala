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
package io.sparkcyclone.spark.codegen.groupby

import io.sparkcyclone.spark.codegen.SparkExpressionToCExpression.EvalFallback
import org.scalatest.freespec.AnyFreeSpec
import ConvertNamedExpression._
import io.sparkcyclone.native.code._
import io.sparkcyclone.spark.codegen.groupby.GroupByOutline.StagedProjection
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  ExprId,
  PrettyAttribute
}
import org.apache.spark.sql.types.DoubleType

final class ConvertNamedExpressionSpec extends AnyFreeSpec {
  private implicit val evalFallback: EvalFallback = EvalFallback.noOp
  "For Staged projection" - {
    "Matches AttributeReference" in {
      val ar = AttributeReference("testx", DoubleType)(exprId = ExprId(5L), qualifier = Seq.empty)
      val result = mapNamedStagedProjection(
        namedExpression = ar,
        idx = 9,
        childAttributes = Seq[Attribute](ar)
      )
      assert(result.contains(StagedProjection("sp_9", VeNullableDouble) -> ar))
    }
    "Matches Alias" in {
      val ar = AttributeReference("testx", DoubleType)(exprId = ExprId(5L), qualifier = Seq.empty)
      val result = mapNamedStagedProjection(
        namedExpression = Alias(child = ar, name = "abcd")(),
        idx = 9,
        childAttributes = Seq[Attribute](ar)
      )
      assert(result.contains(StagedProjection("sp_9", VeNullableDouble) -> ar))
    }
    "Fails to match when child attribute is not there for an attribute reference" in {
      val result = mapNamedStagedProjection(
        namedExpression =
          AttributeReference("testx", DoubleType)(exprId = ExprId(5L), qualifier = Seq.empty),
        idx = 9,
        childAttributes = Seq[Attribute](
          AttributeReference("testx", DoubleType)(exprId = ExprId(6L), qualifier = Seq.empty)
        )
      )
      assert(result.isLeft)
    }
  }

  "For indexed aggregate" - {
    // not worth testing this as there's little complexity in this method for indexedAggregate
  }
}
