package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CExpression}
import com.nec.spark.agile.DeclarativeAggregationConverter.eval
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  AttributeReference,
  BinaryArithmetic,
  Coalesce,
  Expression,
  Literal
}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

/**
 * Many of Spark's aggregations can be reduced to repetition of simple operations.
 * Here we perform exactly that.
 */
final case class DeclarativeAggregationConverter(declarativeAggregate: DeclarativeAggregate)
  extends Aggregation {
  override def initial(prefix: String): CodeLines = {
    CodeLines.from(declarativeAggregate.initialValues.zipWithIndex.map {
      case (Literal(null, _), idx) =>
        s"double ${prefix}_${idx}_${declarativeAggregate.prettyName} = 0;"
      case other => s"$other"
    }.toList)
  }

  private def transformInitial(prefix: String): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference if ar.name == "sum" =>
      ar.withName(s"${prefix}_0_${ar.name}")
    case other =>
      other
  }

  override def iterate(prefix: String): CodeLines =
    CodeLines.from(declarativeAggregate.updateExpressions.zipWithIndex.map { case (e, idx) =>
      s"${prefix}_${idx}_${declarativeAggregate.prettyName} = ${eval(e.transform(transformInitial(prefix))).cCode};"
    }.toList)

  override def compute(prefix: String): CodeLines =
    CodeLines.empty

  override def fetch(prefix: String): CExpression =
    eval(declarativeAggregate.evaluateExpression.transform(transformInitial(prefix)))

  override def free(prefix: String): CodeLines = CodeLines.empty
}

object DeclarativeAggregationConverter {

  private def eval(expression: Expression): CExpression = {
    expression match {
      case b: BinaryArithmetic =>
        CExpression(s"${eval(b.left).cCode} ${b.symbol} ${eval(b.right).cCode}", None)
      case Coalesce(s) =>
        eval(s.head)
      case ar: AttributeReference =>
        CExpression(ar.name, None)
      case _ =>
        CExpression(expression.getClass.getCanonicalName + ": " + expression.toString(), None)
    }
  }

}
