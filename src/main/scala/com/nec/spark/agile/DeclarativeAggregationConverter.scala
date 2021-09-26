package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{Aggregation, CExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}

/**
 * Many of Spark's aggregations can be reduced to repetition of simple operations.
 * Here we perform exactly that.
 */
final case class DeclarativeAggregationConverter(declarativeAggregate: DeclarativeAggregate)
  extends Aggregation {
  override def initial(prefix: String): CodeLines = {
    CodeLines.from(
      declarativeAggregate.initialValues.zipWithIndex
        .zip(declarativeAggregate.aggBufferAttributes)
        .map {
          case ((Literal(null, tpe), idx), att) =>
            CodeLines.from(
              s"${SparkVeMapper.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_${att.name}_nullable = 0;",
              s"${SparkVeMapper.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_${att.name}_nullable_is_set = 0;"
            )
          case ((Literal(other, tpe), idx), att) =>
            CodeLines.from(
              s"${SparkVeMapper.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_${att.name}_nullable = ${other};",
              s"${SparkVeMapper.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_${att.name}_nullable_is_set = 1;"
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

    CodeLines.from(abbs.map { case ((e, idx), aggb) =>
      val codeEval = SparkVeMapper.eval(e)
      codeEval.isNotNullCode match {
        case None =>
          CodeLines.from(
            s"${prefix}_${aggb.name}_nullable = ${codeEval.cCode};",
            s"${prefix}_${aggb.name}_nullable_is_set = 1;"
          )
        case Some(notNullCode) =>
          CodeLines.from(
            s"if (${notNullCode}) {",
            CodeLines
              .from(
                s"${prefix}_${aggb.name}_nullable = ${codeEval.cCode};",
                s"${prefix}_${aggb.name}_nullable_is_set = 1;"
              )
              .indented,
            "}"
          )
      }
    }.toList)
  }

  override def compute(prefix: String): CodeLines =
    CodeLines.empty

  override def fetch(prefix: String): CExpression = {
    SparkVeMapper.eval(declarativeAggregate.evaluateExpression.transform(transformInitial(prefix)))
  }

  override def free(prefix: String): CodeLines = CodeLines.empty
}

object DeclarativeAggregationConverter {}
