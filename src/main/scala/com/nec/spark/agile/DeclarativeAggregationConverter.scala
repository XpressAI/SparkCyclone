package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CExpression, DelegatingAggregation}
import com.nec.spark.agile.SparkVeMapper.EvalFallback
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}

/**
 * Many of Spark's aggregations can be reduced to repetition of simple operations.
 * Here we perform exactly that.
 */
final case class DeclarativeAggregationConverter(
  declarativeAggregate: DeclarativeAggregate,
  evalFallback: EvalFallback
) extends Aggregation {
  override def initial(prefix: String): CodeLines = {
    CodeLines.from(
      declarativeAggregate.initialValues.zipWithIndex
        .zip(declarativeAggregate.aggBufferAttributes)
        .map {
          case ((Literal(null, tpe), idx), att) =>
            CodeLines.from(
              s"${SparkVeMapper.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_${att.name}_nullable = 0;",
              s"int ${prefix}_${att.name}_nullable_is_set = 0;"
            )
          case ((Literal(other, tpe), idx), att) =>
            CodeLines.from(
              s"${SparkVeMapper.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_${att.name}_nullable = ${other};",
              s"int ${prefix}_${att.name}_nullable_is_set = 1;"
            )
          case (other, idx) =>
            sys.error(s"Not supported declarative aggregate input: ${other}")
        }
        .toList
    )
  }

  private def transformInitial(prefix: String): PartialFunction[Expression, Expression] = {
    case ar: AttributeReference
        if declarativeAggregate.aggBufferAttributes.exists(_.name == ar.name) =>
      ar.withName(s"${prefix}_${ar.name}_nullable")
  }

  override def iterate(prefix: String): CodeLines = {
    val abbs = declarativeAggregate.updateExpressions
      .map(e => e.transform(transformInitial(prefix)))
      .zipWithIndex
      .zip(declarativeAggregate.aggBufferAttributes)

    CodeLines.from(
      abbs.map { case ((e, idx), aggb) =>
        val codeEval = SparkVeMapper.eval(e, evalFallback)
        codeEval.isNotNullCode match {
          case None =>
            CodeLines.from(
              s"${SparkVeMapper.sparkTypeToScalarVeType(aggb.dataType).cScalarType} tmp_${prefix}_${aggb.name}_nullable = ${codeEval.cCode};",
              s"int tmp_${prefix}_${aggb.name}_nullable_is_set = ${prefix}_${aggb.name}_nullable_is_set;"
            )
          case Some(notNullCode) =>
            CodeLines.from(
              s"${SparkVeMapper.sparkTypeToScalarVeType(aggb.dataType).cScalarType} tmp_${prefix}_${aggb.name}_nullable = ${codeEval.cCode};",
              s"int tmp_${prefix}_${aggb.name}_nullable_is_set = ${prefix}_${aggb.name}_nullable_is_set;",
              s"if (${notNullCode}) {",
              CodeLines
                .from(
                  s"tmp_${prefix}_${aggb.name}_nullable = ${codeEval.cCode};",
                  s"tmp_${prefix}_${aggb.name}_nullable_is_set = 1;"
                )
                .indented,
              "}"
            )
        }
      }.toList,
      abbs.map { case ((e, idx), aggb) =>
        val codeEval = SparkVeMapper.eval(e, evalFallback)
        codeEval.isNotNullCode match {
          case None =>
            CodeLines.from(
              s"${prefix}_${aggb.name}_nullable = tmp_${prefix}_${aggb.name}_nullable;",
              s"${prefix}_${aggb.name}_nullable_is_set = tmp_${prefix}_${aggb.name}_nullable_is_set;"
            )
          case Some(notNullCode) =>
            CodeLines.from(
              s"${prefix}_${aggb.name}_nullable = tmp_${prefix}_${aggb.name}_nullable;",
              s"${prefix}_${aggb.name}_nullable_is_set = tmp_${prefix}_${aggb.name}_nullable_is_set;"
            )
        }
      }.toList
    )
  }

  override def compute(prefix: String): CodeLines =
    CodeLines.empty

  override def fetch(prefix: String): CExpression = {
    SparkVeMapper.eval(
      declarativeAggregate.evaluateExpression.transform(transformInitial(prefix)),
      evalFallback
    )
  }

  override def free(prefix: String): CodeLines = CodeLines.empty
}

object DeclarativeAggregationConverter {

  private final case class TransformingAggregation(
    declarativeAggregationConverter: DeclarativeAggregationConverter,
    transformResult: CExpression => CExpression
  ) extends DelegatingAggregation(declarativeAggregationConverter) {
    override def fetch(prefix: String): CExpression = {
      transformResult(super.fetch(prefix))
    }
  }

  object AggregateHole extends UnresolvedAttribute(Seq.empty)

  def transformingFetch(expression: Expression, fallback: EvalFallback): Option[Aggregation] = {
    expression.collectFirst {
      case ae @ AggregateExpression(d: DeclarativeAggregate, mode, isDistinct, filter, resultId) =>
        val conv = DeclarativeAggregationConverter(d, fallback)
        TransformingAggregation(
          conv,
          originalResult =>
            SparkVeMapper.eval(
              expression.transformDown { case `ae` =>
                AggregateHole
              },
              EvalFallback
                .from { case AggregateHole =>
                  originalResult
                }
                .orElse(fallback)
            )
        )
    }
  }

}
