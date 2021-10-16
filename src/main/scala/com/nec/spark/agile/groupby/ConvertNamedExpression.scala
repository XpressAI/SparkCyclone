package com.nec.spark.agile.groupby

import com.nec.spark.agile.CFunctionGeneration.{Aggregation, GroupByExpression, TypedCExpression2}
import com.nec.spark.agile.{DeclarativeAggregationConverter, SparkExpressionToCExpression}
import com.nec.spark.agile.SparkExpressionToCExpression.{sparkTypeToVeType, EvalFallback}
import com.nec.spark.agile.groupby.GroupByOutline.{
  StagedAggregation,
  StagedAggregationAttribute,
  StagedProjection,
  StringReference
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StringType

/**
 * All the logic to convert from Spark's NamedExpression to C Expressions OR Aggregations, encompassing Aliases,
 * DeclarativeAggregates, and other possibilities
 */
object ConvertNamedExpression {
  import com.nec.spark.agile.SparkExpressionToCExpression.EvaluationAttempt._
  def doProj(e: Expression)(implicit
    evalFallback: EvalFallback
  ): Either[String, Either[StringReference, TypedCExpression2]] =
    e match {
      case ar: AttributeReference if ar.dataType == StringType =>
        Right(Left(StringReference(ar.name)))
      case Alias(ar: AttributeReference, _) if ar.dataType == StringType =>
        Right(Left(StringReference(ar.name)))
      case Alias(other, _) =>
        SparkExpressionToCExpression
          .eval(other)
          .reportToString
          .map(ce =>
            Right(TypedCExpression2(SparkExpressionToCExpression.sparkTypeToScalarVeType(other.dataType), ce))
          )
      case other =>
        SparkExpressionToCExpression
          .eval(other)
          .reportToString
          .map(ce =>
            Right(TypedCExpression2(SparkExpressionToCExpression.sparkTypeToScalarVeType(other.dataType), ce))
          )
    }

  def mapNamedExp(
    namedExpression: NamedExpression,
    idx: Int,
    referenceReplacer: PartialFunction[Expression, Expression],
    child: LogicalPlan
  )(implicit
    evalFallback: EvalFallback
  ): Either[String, Either[(StagedAggregation, Expression), (StagedProjection, Expression)]] = {
    computeIndexedAggregate(namedExpression, idx, referenceReplacer) match {
      case Some(c) => c.map(sae => Left(sae))
      case None =>
        namedExpression match {
          case ar: AttributeReference if child.output.toList.exists(_.exprId == ar.exprId) =>
            Right(Right(StagedProjection(s"sp_${idx}", sparkTypeToVeType(ar.dataType)) -> ar))
          case Alias(exp, _) =>
            Right(Right(StagedProjection(s"sp_${idx}", sparkTypeToVeType(exp.dataType)) -> exp))
          case other =>
            Left(s"Unexpected aggregate expression: ${other}, type ${other.getClass}")
        }
    }
  }

  private def computeIndexedAggregate(
    aggregateExpression: NamedExpression,
    index: Int,
    referenceReplacer: PartialFunction[Expression, Expression]
  )(implicit
    evalFallback: EvalFallback
  ): Option[Either[String, (StagedAggregation, Expression)]] = {
    PartialFunction
      .condOpt(aggregateExpression) {
        case o @ Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _) =>
          Right {
            (
              GroupByExpression.GroupByAggregation(
                DeclarativeAggregationConverter(
                  d.transform(referenceReplacer).asInstanceOf[DeclarativeAggregate]
                )
              ),
              o
            )
          }
        case Alias(other, _) if other.collectFirst { case _: DeclarativeAggregate =>
              ()
            }.nonEmpty =>
          DeclarativeAggregationConverter
            .transformingFetch(other.transform(referenceReplacer))
            .toRight(s"Cannot figure out how to replace: ${other} (${other.getClass})")
            .map(ag => (GroupByExpression.GroupByAggregation(ag), other))
        case other if other.collectFirst { case _: DeclarativeAggregate =>
              ()
            }.nonEmpty =>
          DeclarativeAggregationConverter
            .transformingFetch(other.transform(referenceReplacer))
            .toRight(s"Cannot figure out how to replace: ${other} (${other.getClass})")
            .map(ag => (GroupByExpression.GroupByAggregation(ag), other))
      }
      .map(_.map { case (groupByExpression, expr) =>
        StagedAggregation(
          s"agg_${index}",
          sparkTypeToVeType(expr.dataType),
          groupByExpression.aggregation.partialValues(s"agg_${index}").map { case (cs, ce) =>
            StagedAggregationAttribute(name = cs.name, veScalarType = cs.veType)
          }
        ) -> expr
      })
  }

  def computeAggregate(
    exp: Expression
  )(implicit evalFallback: EvalFallback): Either[String, Aggregation] = exp match {
    case Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _) =>
      Right(DeclarativeAggregationConverter(d))
    case other =>
      DeclarativeAggregationConverter
        .transformingFetch(other)
        .toRight(s"Cannot figure out how to replace: ${other} (${other.getClass})")
  }

  def mapGroupingExpression(expr: Expression, refRep: PartialFunction[Expression, Expression])(
    implicit evalFallback: EvalFallback
  ): Either[String, Either[StringReference, TypedCExpression2]] = {
    expr.dataType match {
      case StringType =>
        /**
         * This is not correct for any group-by that are not a simple reference.
         * todo fix it
         */
        expr
          .find(_.isInstanceOf[AttributeReference])
          .flatMap(expr =>
            expr
              .transform(refRep)
              .collectFirst {
                case ar: AttributeReference if ar.name.contains("input_") =>
                  Left(StringReference(ar.name.replaceAllLiterally("->data[i]", "")))
              }
          )
          .toRight(s"Cannot support group by: ${expr} (type: ${expr.dataType})")
      case _ =>
        SparkExpressionToCExpression
          .eval(expr.transform(refRep))
          .reportToString
          .map(ce =>
            Right(
              TypedCExpression2(
                veType = SparkExpressionToCExpression.sparkTypeToScalarVeType(expr.dataType),
                cExpression = ce
              )
            )
          )
    }
  }

}
