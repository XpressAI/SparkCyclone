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
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.planning.VERewriteStrategy.{AggPrefix, StagedProjectionPrefix}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
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
            Right(
              TypedCExpression2(
                SparkExpressionToCExpression.sparkTypeToScalarVeType(other.dataType),
                ce
              )
            )
          )
      case other =>
        SparkExpressionToCExpression
          .eval(other)
          .reportToString
          .map(ce =>
            Right(
              TypedCExpression2(
                SparkExpressionToCExpression.sparkTypeToScalarVeType(other.dataType),
                ce
              )
            )
          )
    }

  def mapNamedExp(
    namedExpression: NamedExpression,
    idx: Int,
    referenceReplacer: PartialFunction[Expression, Expression],
    childAttributes: Seq[Attribute]
  )(implicit
    evalFallback: EvalFallback
  ): Either[String, Either[(StagedAggregation, Expression), (StagedProjection, Expression)]] =
    computeIndexedAggregate(namedExpression, idx, referenceReplacer) match {
      case Some(c) => c.map(sae => Left(sae))
      case None =>
        mapNamedStagedProjection(
          namedExpression = namedExpression,
          idx = idx,
          childAttributes = childAttributes
        ).map(r => Right(r))
    }

  def mapNamedStagedProjection(
    namedExpression: NamedExpression,
    idx: Int,
    childAttributes: Seq[Attribute]
  ): Either[String, (StagedProjection, Expression)] = {
    namedExpression match {
      case ar: AttributeReference if childAttributes.toList.exists(_.exprId == ar.exprId) =>
        Right(
          StagedProjection(s"${StagedProjectionPrefix}${idx}", sparkTypeToVeType(ar.dataType)) -> ar
        )
      case Alias(exp, _) =>
        Right(
          StagedProjection(
            s"${StagedProjectionPrefix}${idx}",
            sparkTypeToVeType(exp.dataType)
          ) -> exp
        )
      case other =>
        Left(s"Unexpected aggregate expression: ${other}, type ${other.getClass}")
    }
  }

  def computeIndexedAggregate(
    aggregateExpression: NamedExpression,
    index: Int,
    referenceReplacer: PartialFunction[Expression, Expression]
  )(implicit evalFallback: EvalFallback): Option[Either[String, (StagedAggregation, Expression)]] =
    extractAggregates(aggregateExpression, referenceReplacer)
      .map(_.map { case (groupByExpression, expr) =>
        StagedAggregation(
          s"${AggPrefix}${index}",
          sparkTypeToVeType(expr.dataType),
          groupByExpression.aggregation.partialValues(s"$AggPrefix$index").map {
            case (cScalarVector, _) =>
              StagedAggregationAttribute(
                name = cScalarVector.name,
                veScalarType = cScalarVector.veType
              )
          }
        ) -> expr
      })

  private def extractAggregates(
    aggregateExpression: NamedExpression,
    referenceReplacer: PartialFunction[Expression, Expression]
  )(implicit
    evalFallback: EvalFallback
  ): Option[Either[String, (GroupByExpression.GroupByAggregation, Expression)]] =
    PartialFunction
      .condOpt(aggregateExpression) {
        case aliasOfAggregateExpression @ Alias(
              AggregateExpression(declarativeAggregate: DeclarativeAggregate, _, _, _, _),
              _
            ) =>
          Right {
            (
              GroupByExpression.GroupByAggregation(
                DeclarativeAggregationConverter(
                  declarativeAggregate
                    .transform(referenceReplacer)
                    .asInstanceOf[DeclarativeAggregate]
                )
              ),
              aliasOfAggregateExpression
            )
          }
        case HasDeclarativeAggregate(expressionWithDeclarativeAggregate) =>
          DeclarativeAggregationConverter
            .transformingFetch(expressionWithDeclarativeAggregate.transform(referenceReplacer))
            .toRight(
              s"Cannot figure out how to replace: ${expressionWithDeclarativeAggregate} (${expressionWithDeclarativeAggregate.getClass})"
            )
            .map(aggregation =>
              (
                GroupByExpression.GroupByAggregation(aggregation),
                expressionWithDeclarativeAggregate
              )
            )
      }

  object HasDeclarativeAggregate {
    def unapply(namedExpression: NamedExpression): Option[Expression] =
      PartialFunction.condOpt(namedExpression) {
        case Alias(expressionWithDeclarativeAggregate, _)
            if expressionWithDeclarativeAggregate.collectFirst { case _: DeclarativeAggregate =>
              ()
            }.nonEmpty =>
          expressionWithDeclarativeAggregate

        case expressionWithDeclarativeAggregate if expressionWithDeclarativeAggregate.collectFirst {
              case _: DeclarativeAggregate =>
                ()
            }.nonEmpty =>
          expressionWithDeclarativeAggregate
      }
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
                case ar: AttributeReference if ar.name.contains(VERewriteStrategy.InputPrefix) =>
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
