package com.nec.cmake.eval

import com.nec.cmake.eval.SparkToVeAggregatorSpec.fromDeclarativeAggregate
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CExpression}
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  AttributeReference,
  Coalesce,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Sum}
import org.apache.spark.sql.types.DoubleType
import org.scalatest.freespec.AnyFreeSpec

object SparkToVeAggregatorSpec {

  def eval(expression: Expression): CExpression = {
    expression match {
      case Add(Coalesce(Seq(x, y)), AttributeReference(name, _, _, _), false) =>
        CExpression(name, None)
      case _ =>
        CExpression(expression.toString(), None)
    }
  }

  def fromDeclarativeAggregate(declarativeAggregate: DeclarativeAggregate): Aggregation =
    new Aggregation {
      override def initial(prefix: String): CodeLines = {
        CodeLines.from(declarativeAggregate.initialValues.zipWithIndex.map {
          case (Literal(null, _), idx) =>
            s"double ${prefix}_${idx}_${declarativeAggregate.prettyName} = 0;"
          case other => s"$other"
        }.toList)
      }

      override def iterate(prefix: String): CodeLines =
        CodeLines.from(declarativeAggregate.updateExpressions.zipWithIndex.map { case (e, idx) =>
          s"${prefix}_${idx}_${declarativeAggregate.prettyName} += ${eval(e).cCode};"
        }.toList)

      override def compute(prefix: String): CodeLines =
        CodeLines.empty

      override def fetch(prefix: String): CExpression =
        eval(declarativeAggregate.evaluateExpression.transform {
          case ar: AttributeReference if ar.name == "sum#1" =>
            ar.withName(s"${prefix}_0_${declarativeAggregate.prettyName}")
          case other => other
        })

      override def free(prefix: String): CodeLines = CodeLines.empty
    }
}
final class SparkToVeAggregatorSpec extends AnyFreeSpec {
  val as = Aggregation.sum(CExpression("abc", None))
  "sum works" - {
    "initial is computed" in {
      assert(as.initial("x") == CodeLines.from("double x_aggregate_sum = 0;"))
    }
    "compute is computed" in {
      assert(as.compute("x") == CodeLines.empty)
    }
    "iterate is computed" in {
      assert(as.iterate("x") == CodeLines.from("x_aggregate_sum += abc;"))
    }
    "fetch is computed" in {
      assert(as.fetch("x") == CExpression("x_aggregate_sum", None))
    }
    "free is computed" in {
      assert(as.free("x") == CodeLines.empty)
    }
  }

  "Declarative-aggregate based" - {
    val res = fromDeclarativeAggregate(
      Sum(AttributeReference("input_0->data[i]", DoubleType, nullable = false)())
    )

    "initial is computed" in {
      assert(res.initial("x") == CodeLines.from("double x_0_sum = 0;"))
    }
    "compute is computed" in {
      assert(res.compute("x") == CodeLines.empty)
    }
    "iterate is computed" in {
      assert(res.iterate("x") == CodeLines.from("x_0_sum += input_0->data[i];"))
    }
    "fetch is computed" in {
      assert(res.fetch("x") == CExpression("x_0_sum", None))
    }
    "free is computed" in {
      assert(res.free("x") == CodeLines.empty)
    }
  }
}
