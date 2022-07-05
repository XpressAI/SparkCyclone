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
package io.sparkcyclone.spark.agile

import io.sparkcyclone.spark.agile.core.{CodeLines, CScalarVector}
import io.sparkcyclone.spark.agile.CFunctionGeneration._
import io.sparkcyclone.spark.agile.SparkExpressionToCExpression.EvalFallback
import io.sparkcyclone.spark.agile.SparkExpressionToCExpression.EvaluationAttempt._
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Expression,
  LeafExpression,
  Literal,
  Unevaluable
}
import org.apache.spark.sql.types.DataType

/**
 * Many of Spark's aggregations can be reduced to repetition of simple operations.
 * Here we perform exactly that.
 */
final case class DeclarativeAggregationConverter(declarativeAggregate: DeclarativeAggregate)(
  implicit evalFallback: EvalFallback
) extends Aggregation {
  override def initial(prefix: String): CodeLines = {
    CodeLines.from(
      declarativeAggregate.initialValues.zipWithIndex
        .zip(declarativeAggregate.aggBufferAttributes)
        .map {
          case ((Literal(null, tpe), idx), att) =>
            CodeLines.from(
              s"${SparkExpressionToCExpression.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_attr_${att.name}_nullable = 0;",
              s"int ${prefix}_attr_${att.name}_nullable_is_set = 0;"
            )
          case ((Literal(other, tpe), idx), att) =>
            CodeLines.from(
              s"${SparkExpressionToCExpression.sparkTypeToScalarVeType(tpe).cScalarType} ${prefix}_attr_${att.name}_nullable = ${other};",
              s"int ${prefix}_attr_${att.name}_nullable_is_set = 1;"
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
      ar.withName(s"${prefix}_attr_${ar.name}_nullable")
  }

  override def iterate(prefix: String): CodeLines = {
    val abbs = declarativeAggregate.updateExpressions
      .map(e => e.transform(transformInitial(prefix)))
      .zipWithIndex
      .zip(declarativeAggregate.aggBufferAttributes)

    CodeLines.from(
      abbs.map { case ((e, idx), aggb) =>
        val codeEval = SparkExpressionToCExpression.eval(e)(evalFallback).getOrReport()
        codeEval.isNotNullCode match {
          case None =>
            CodeLines.from(
              s"${SparkExpressionToCExpression
                .sparkTypeToScalarVeType(aggb.dataType)
                .cScalarType} tmp_${prefix}_attr_${aggb.name}_nullable = ${codeEval.cCode};",
              s"int tmp_${prefix}_attr_${aggb.name}_nullable_is_set = 1;"
            )
          case Some(notNullCode) =>
            CodeLines.from(
              s"${SparkExpressionToCExpression
                .sparkTypeToScalarVeType(aggb.dataType)
                .cScalarType} tmp_${prefix}_attr_${aggb.name}_nullable = ${codeEval.cCode};",
              s"int tmp_${prefix}_attr_${aggb.name}_nullable_is_set = ${prefix}_attr_${aggb.name}_nullable_is_set;",
              s"if (${notNullCode}) {",
              CodeLines
                .from(
                  s"tmp_${prefix}_attr_${aggb.name}_nullable = ${codeEval.cCode};",
                  s"tmp_${prefix}_attr_${aggb.name}_nullable_is_set = 1;"
                )
                .indented,
              "}"
            )
        }
      }.toList,
      abbs.map { case ((e, idx), aggb) =>
        val codeEval = SparkExpressionToCExpression.eval(e).getOrReport()
        codeEval.isNotNullCode match {
          case None =>
            CodeLines.from(
              s"${prefix}_attr_${aggb.name}_nullable = tmp_${prefix}_attr_${aggb.name}_nullable;",
              s"${prefix}_attr_${aggb.name}_nullable_is_set = tmp_${prefix}_attr_${aggb.name}_nullable_is_set;"
            )
          case Some(notNullCode) =>
            CodeLines.from(
              s"${prefix}_attr_${aggb.name}_nullable = tmp_${prefix}_attr_${aggb.name}_nullable;",
              s"${prefix}_attr_${aggb.name}_nullable_is_set = tmp_${prefix}_attr_${aggb.name}_nullable_is_set;"
            )
        }
      }.toList
    )
  }

  override def compute(prefix: String): CodeLines =
    CodeLines.empty

  override def fetch(prefix: String): CExpression = SparkExpressionToCExpression
    .eval(declarativeAggregate.evaluateExpression.transform(transformInitial(prefix)))
    .getOrReport()

  override def free(prefix: String): CodeLines = CodeLines.empty

  override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
    declarativeAggregate.aggBufferAttributes.map { attRef =>
      (
        CScalarVector(
          s"${prefix}_attr_${attRef.name}",
          SparkExpressionToCExpression.sparkTypeToScalarVeType(attRef.dataType)
        ),
        CExpression(
          s"${prefix}_attr_${attRef.name}_nullable",
          Some(s"${prefix}_attr_${attRef.name}_nullable_is_set")
        )
      )
    }.toList

  override def merge(prefix: String, inputPrefix: String): CodeLines = {
    CodeLines.from(
      declarativeAggregate.mergeExpressions.zipWithIndex
        .zip(declarativeAggregate.aggBufferAttributes)
        .zip(
          DeclarativeAggregationConverter
            .rewriteMerge(prefix, inputPrefix)(declarativeAggregate)
        )
        .map { case (((mergeExp, idx), aggb), cExp) =>
          cExp.isNotNullCode match {
            case None =>
              CodeLines.from(
                s"${SparkExpressionToCExpression
                  .sparkTypeToScalarVeType(aggb.dataType)
                  .cScalarType} tmp_${prefix}_attr_${aggb.name}_nullable = ${cExp.cCode};",
                s"int tmp_${prefix}_attr_${aggb.name}_nullable_is_set = 1;"
              )
            case Some(notNullCode) =>
              CodeLines.from(
                s"${SparkExpressionToCExpression
                  .sparkTypeToScalarVeType(aggb.dataType)
                  .cScalarType} tmp_${prefix}_attr_${aggb.name}_nullable = ${cExp.cCode};",
                s"int tmp_${prefix}_attr_${aggb.name}_nullable_is_set = ${prefix}_attr_${aggb.name}_nullable_is_set;",
                s"if (${notNullCode}) {",
                CodeLines
                  .from(
                    s"tmp_${prefix}_attr_${aggb.name}_nullable = ${cExp.cCode};",
                    s"tmp_${prefix}_attr_${aggb.name}_nullable_is_set = 1;"
                  )
                  .indented,
                "}"
              )
          }
        }
        .toList,
      CodeLines.from(
        declarativeAggregate.mergeExpressions
          .map(e => e.transform(transformInitial(prefix)))
          .zipWithIndex
          .zip(declarativeAggregate.aggBufferAttributes)
          .map { case ((mergeExp, idx), aggb) =>
            val codeEval = SparkExpressionToCExpression.eval(mergeExp).getOrReport()
            codeEval.isNotNullCode match {
              case None =>
                CodeLines.from(
                  s"${prefix}_attr_${aggb.name}_nullable = tmp_${prefix}_attr_${aggb.name}_nullable;",
                  s"${prefix}_attr_${aggb.name}_nullable_is_set = tmp_${prefix}_attr_${aggb.name}_nullable_is_set;"
                )
              case Some(notNullCode) =>
                CodeLines.from(
                  s"${prefix}_attr_${aggb.name}_nullable = tmp_${prefix}_attr_${aggb.name}_nullable;",
                  s"${prefix}_attr_${aggb.name}_nullable_is_set = tmp_${prefix}_attr_${aggb.name}_nullable_is_set;"
                )
            }
          }
          .toList
      )
    )
  }
}

object DeclarativeAggregationConverter {
  def rewriteMerge(prefix: String, inputPrefix: String)(
    declarativeAggregate: DeclarativeAggregate
  )(implicit evalFallback: EvalFallback): List[CExpression] = {
    declarativeAggregate.aggBufferAttributes.zip(declarativeAggregate.mergeExpressions).map {
      case (targetAttribute, mergeExpression) =>
        SparkExpressionToCExpression
          .eval(mergeExpression)(
            EvalFallback
              .from {
                case aggregateAttribute
                    if declarativeAggregate.aggBufferAttributes.contains(aggregateAttribute) =>
                  CExpression(
                    cCode = s"${prefix}_attr_${targetAttribute.name}_nullable",
                    isNotNullCode = Some(s"${prefix}_attr_${targetAttribute.name}_nullable_is_set")
                  )
                case inputAggregateAttribute
                    if declarativeAggregate.inputAggBufferAttributes
                      .contains(inputAggregateAttribute) =>
                  CExpression(
                    cCode = s"${inputPrefix}_attr_${targetAttribute.name}->data[i]",
                    isNotNullCode = Some(
                      s"${inputPrefix}_attr_${targetAttribute.name}->get_validity(i)"
                    )
                  )
              }
              .orElse(evalFallback)
          )
          .getOrReport()
    }
  }.toList

  private final case class TransformingAggregation(
    declarativeAggregationConverter: DeclarativeAggregationConverter,
    transformResult: CExpression => CExpression
  ) extends DelegatingAggregation(declarativeAggregationConverter) {
    override def fetch(prefix: String): CExpression = {
      transformResult(super.fetch(prefix))
    }
  }

  private final case class CombinedAggregation(
    underlying: List[Aggregation],
    combineResults: List[CExpression] => CExpression
  ) extends Aggregation {
    override def initial(prefix: String): CodeLines =
      CodeLines.from(underlying.map(_.initial(prefix)))

    override def iterate(prefix: String): CodeLines =
      CodeLines.from(underlying.map(_.iterate(prefix)))

    override def compute(prefix: String): CodeLines =
      CodeLines.from(underlying.map(_.compute(prefix)))

    override def fetch(prefix: String): CExpression =
      combineResults(underlying.map(_.fetch(prefix)))

    override def free(prefix: String): CodeLines = CodeLines.from(underlying.map(_.free(prefix)))

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      underlying.flatMap(_.partialValues(prefix))

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      underlying.map(_.merge(prefix, inputPrefix)).reduce(_ ++ _)
  }

  final case class AggregateHole(aggregateExpression: AggregateExpression)
    extends LeafExpression
    with Unevaluable {
    override def nullable: Boolean = aggregateExpression.nullable

    override def dataType: DataType = aggregateExpression.dataType
  }

  def transformingFetch(
    expression: Expression
  )(implicit fallback: EvalFallback): Option[Aggregation] = {
    val allAggregates = expression.collect {
      case ae @ AggregateExpression(d: DeclarativeAggregate, mode, isDistinct, filter, resultId) =>
        (ae, d)
    }

    val aggregations: List[(AggregateExpression, Aggregation)] = allAggregates.zipWithIndex.map {
      case ((ae, de), idx) =>
        ae -> SuffixedAggregation(s"_$idx", DeclarativeAggregationConverter(de))
    }.toList

    val aeToAggregation: Map[AggregateExpression, Aggregation] = aggregations.toMap

    val cheese = expression.transformDown {
      case ae: AggregateExpression if aeToAggregation.contains(ae) =>
        AggregateHole(ae)
    }

    Option {
      CombinedAggregation(
        underlying = aggregations.map(_._2),
        combineResults = results => {
          SparkExpressionToCExpression
            .eval(cheese)(
              EvalFallback
                .from {
                  case AggregateHole(h) if aeToAggregation.contains(h) =>
                    results.apply(aggregations.indexWhere { case (_ae, _ag) => _ae == h })
                }
                .orElse(fallback)
            )
            .getOrReport()
        }
      )
    }
  }
}
