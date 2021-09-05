package com.nec.spark.agile

import com.nec.arrow.ArrowNativeInterface
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.ExprEvaluation2.{CExpression, CVector, NamedTypedCExpression, VeProjection, VeType}
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

  /** This will replace VERewriteStrategy and that will contain all the Spark-related mappings, rather than the C code generator.
   * It will have some code generation, however it is strictly scoped to expression evaluation than anything else.
   * */
  def toVeTransformation(qr: LogicalPlan): VeProjection[CVector, NamedTypedCExpression] = qr match {
    case proj@logical.Project(resultExpressions, child) if !resultExpressions.forall {
      /** If it's just a rename, don't send to VE * */
      case a: Alias if a.child.isInstanceOf[Attribute] => true
      case a: AttributeReference => true
      case _ => false
    } =>
      VeProjection(
        inputs = child.output.toList.zipWithIndex.map { case (attr, idx) =>
          CVector(s"input_$idx", CExpressionEvaluation.veType(attr.dataType))
        },
        outputs = resultExpressions.zipWithIndex.map { case (ne, idx) =>
          NamedTypedCExpression(s"output_$idx", CExpressionEvaluation.veType(ne.dataType), CExpression(CExpressionEvaluation.evaluateExpression(proj.references.toList, ne), isNotNullCode = None))
        }.toList
      )
    case _ => sys.error("Not matched")
  }

  object NoopEvaluator extends NativeEvaluator {
    override def forCode(code: String): ArrowNativeInterface = throw new NotImplementedError("Intentionally not implemented")
  }
}

final class LogicalPlanMatchingSpec extends AnyFreeSpec {

  "parse select-project" in {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.apache.spark.sql.catalyst.dsl.plans._
    val result = LocalRelation(
      AttributeReference("a", DoubleType)(),
      AttributeReference("b", DoubleType)())
      .select($"a" * $"b", $"a" + 1.0)
    val testAnalyzer = new Analyzer(
      new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin))
    val qr = testAnalyzer.execute(result)

    assert(new VERewriteStrategy(NoopEvaluator).apply(qr).nonEmpty, "Plan is supported already by the VE Rewrite Strategy")

    assert(toVeTransformation(qr) == VeProjection(
      inputs = List(
        CVector("input_0", VeType.veDouble),
        CVector("input_1", VeType.veDouble)),
      outputs = List(
        NamedTypedCExpression("output_0", VeType.VeNullableDouble, CExpression("input_0->data[i] * input_1->data[i]", isNotNullCode = None)),
        NamedTypedCExpression("output_1", VeType.VeNullableDouble, CExpression("input_0->data[i] + 1.0", isNotNullCode = None))
      )
    ))
  }

}
