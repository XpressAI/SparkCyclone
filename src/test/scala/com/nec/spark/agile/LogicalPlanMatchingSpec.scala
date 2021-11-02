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

import com.nec.arrow.ArrowNativeInterface
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration.{CExpression, CScalarVector, NamedTypedCExpression, VeProjection, VeScalarType}
import com.nec.spark.agile.LogicalPlanMatchingSpec.{NoopEvaluator, toVeTransformation}
import com.nec.spark.planning.VERewriteStrategy

import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec

/**
 * This spec is created to match various possibilities of rewrite strategies in a
 * way that does not need the full Spark context
 */
object LogicalPlanMatchingSpec {

  /**
   * This will replace VERewriteStrategy and that will contain all the Spark-related mappings, rather than the C code generator.
   * It will have some code generation, however it is strictly scoped to expression evaluation than anything else.
   */
  def toVeTransformation(qr: LogicalPlan): VeProjection[CScalarVector, NamedTypedCExpression] = qr match {
    case proj @ logical.Project(resultExpressions, child) if !resultExpressions.forall {
          /** If it's just a rename, don't send to VE * */
          case a: Alias if a.child.isInstanceOf[Attribute] => true
          case a: AttributeReference                       => true
          case _                                           => false
        } =>
      VeProjection(
        inputs = child.output.toList.zipWithIndex.map { case (attr, idx) =>
          CScalarVector(s"input_$idx", CFunctionGeneration.veType(attr.dataType))
        },
        outputs = resultExpressions.zipWithIndex.map { case (ne, idx) =>
          NamedTypedCExpression(
            s"output_$idx",
            CFunctionGeneration.veType(ne.dataType),
            CExpression(
              CExpressionEvaluation.evaluateExpression(proj.references.toList, ne),
              isNotNullCode = None
            )
          )
        }.toList
      )
    case _ => sys.error("Not matched")
  }

  object NoopEvaluator extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterface = throw new NotImplementedError(
      "Intentionally not implemented"
    )
  }
}

final class LogicalPlanMatchingSpec extends AnyFreeSpec {

  "parse select-project" ignore {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.apache.spark.sql.catalyst.dsl.plans._
    val result =
      LocalRelation(AttributeReference("a", DoubleType)(), AttributeReference("b", DoubleType)())
        .select($"a" * $"b", $"a" + 1.0)
    val testAnalyzer =
      new Analyzer(new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))
    val qr = testAnalyzer.execute(result)

    assert(
      new VERewriteStrategy(NoopEvaluator).apply(qr).nonEmpty,
      "Plan is supported already by the VE Rewrite Strategy"
    )

    assert(
      toVeTransformation(qr) == VeProjection(
        inputs = List(
          CScalarVector("input_0", VeScalarType.veNullableDouble),
          CScalarVector("input_1", VeScalarType.veNullableDouble)
        ),
        outputs = List(
          NamedTypedCExpression(
            "output_0",
            VeScalarType.VeNullableDouble,
            CExpression("input_0->data[i] * input_1->data[i]", isNotNullCode = None)
          ),
          NamedTypedCExpression(
            "output_1",
            VeScalarType.VeNullableDouble,
            CExpression("input_0->data[i] + 1.0", isNotNullCode = None)
          )
        )
      )
    )
  }

}
