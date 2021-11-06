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
package com.nec.spark.planning

import com.nec.native.NativeEvaluator
import com.nec.spark.agile.{CFunctionGeneration, SparkExpressionToCExpression}
import com.nec.spark.agile.SparkExpressionToCExpression.{
  eval,
  replaceReferences,
  sparkSortDirectionToSortOrdering,
  sparkTypeToScalarVeType,
  sparkTypeToVeType,
  EvalFallback
}
import com.nec.spark.agile.groupby.ConvertNamedExpression.{computeAggregate, mapGroupingExpression}
import com.nec.spark.agile.groupby.GroupByOutline.{GroupingKey, StagedProjection}
import com.nec.spark.agile.groupby.{
  ConvertNamedExpression,
  GroupByOutline,
  GroupByPartialGenerator,
  GroupByPartialToFinalGenerator
}
import com.nec.spark.planning.NativeAggregationEvaluationPlan.EvaluationMode
import com.nec.spark.planning.VERewriteStrategy.{
  GroupPrefix,
  InputPrefix,
  SequenceList,
  VeRewriteStrategyOptions
}
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  HyperLogLogPlusPlus
}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{REPARTITION, ShuffleExchangeExec}
import scala.collection.immutable
import scala.util.Try

import com.nec.spark.agile.CFunctionGeneration.{
  CExpression,
  CScalarVector,
  TypedCExpression2,
  VeSort,
  VeSortExpression
}
import com.nec.spark.planning.NativeSortEvaluationPlan.SortingMode.Coalesced

object VERewriteStrategy {
  var _enabled: Boolean = true
  var failFast: Boolean = false
  final case class VeRewriteStrategyOptions(
    preShufflePartitions: Option[Int],
    enableVeSorting: Boolean
  )
  object VeRewriteStrategyOptions {
    val default: VeRewriteStrategyOptions =
      VeRewriteStrategyOptions(preShufflePartitions = Some(8), enableVeSorting = false)
  }

  implicit class SequenceList[A, B](l: List[Either[A, B]]) {
    def sequence: Either[A, List[B]] = l.flatMap(_.left.toOption).headOption match {
      case Some(error) => Left(error)
      case None        => Right(l.flatMap(_.right.toOption))
    }
  }
  val StagedProjectionPrefix = "sp_"
  val AggPrefix = "agg_"
  val InputPrefix: String = "input_"
  val GroupPrefix: String = "group_"
}

final case class VERewriteStrategy(
  nativeEvaluator: NativeEvaluator,
  options: VeRewriteStrategyOptions = VeRewriteStrategyOptions.default
) extends Strategy
  with LazyLogging {

  import com.github.ghik.silencer.silent

  @silent
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def functionPrefix: String = s"eval_${Math.abs(plan.toString.hashCode())}"

    val failFast = VERewriteStrategy.failFast

    if (VERewriteStrategy._enabled) {
      log.debug(
        s"Processing input plan with VERewriteStrategy: $plan, output types were: ${plan.output.map(_.dataType)}"
      )

      def res: immutable.Seq[SparkPlan] = plan match {
        case logical.Aggregate(groupingExpressions, aggregateExpressions, child)
            if child.output.nonEmpty &&
              aggregateExpressions.nonEmpty &&
              !Try(
                aggregateExpressions.head
                  .asInstanceOf[Alias]
                  .child
                  .asInstanceOf[AggregateExpression]
                  .aggregateFunction
                  .isInstanceOf[HyperLogLogPlusPlus]
              ).getOrElse(false) =>
          implicit val fallback: EvalFallback = EvalFallback.noOp

          val groupingExpressionsKeys: List[(GroupingKey, Expression)] =
            groupingExpressions.zipWithIndex.map { case (e, i) =>
              (
                GroupingKey(
                  name = s"${GroupPrefix}${i}",
                  veType = SparkExpressionToCExpression.sparkTypeToVeType(e.dataType)
                ),
                e
              )
            }.toList

          val referenceReplacer =
            SparkExpressionToCExpression.referenceReplacer(
              prefix = InputPrefix,
              inputs = child.output.toList
            )

          val inputsList = child.output.zipWithIndex.map { case (att, id) =>
            sparkTypeToVeType(att.dataType).makeCVector(s"${InputPrefix}${id}")
          }.toList

          val evaluationPlanE: Either[String, SparkPlan] = for {
            projections <- aggregateExpressions.zipWithIndex
              .map { case (ne, i) =>
                ConvertNamedExpression
                  .mapNamedExp(ne, i, referenceReplacer, child)
                  .map(_.right.toSeq)
              }
              .toList
              .sequence
              .map(_.flatten)
            aggregates <-
              aggregateExpressions.zipWithIndex
                .map { case (ne, i) =>
                  ConvertNamedExpression
                    .mapNamedExp(ne, i, referenceReplacer, child)
                    .map(_.left.toSeq)
                }
                .toList
                .sequence
                .map(_.flatten)
            validateNamedOutput = { namedExp_ : NamedExpression =>
              val namedExp = namedExp_ match {
                case Alias(child, _) => child
                case _               => namedExp_
              }
              projections
                .collectFirst {
                  case (pk, `namedExp`)           => Left(pk)
                  case (pk, Alias(`namedExp`, _)) => Left(pk)
                }
                .orElse {
                  aggregates.collectFirst {
                    case (agg, `namedExp`) =>
                      Right(agg)
                    case (agg, Alias(`namedExp`, _)) =>
                      Right(agg)
                  }
                }
                .toRight(
                  s"Unmatched output: ${namedExp}; type ${namedExp.getClass}; Spark type ${namedExp.dataType}. Have aggregates: ${aggregates
                    .mkString(",")}"
                )
            }
            finalOutputs <- aggregateExpressions
              .map(validateNamedOutput)
              .toList
              .sequence
            stagedGroupBy = GroupByOutline(
              groupingKeys = groupingExpressionsKeys.map { case (gk, _) => gk },
              finalOutputs = finalOutputs
            )
            _ = logInfo(s"stagedGroupBy = ${stagedGroupBy}")
            computedGroupingKeys <-
              groupingExpressionsKeys.map { case (gk, exp) =>
                mapGroupingExpression(exp, referenceReplacer)
                  .map(e => gk -> e)
              }.sequence
            computedProjections <- projections.map { case (sp, p) =>
              ConvertNamedExpression.doProj(p.transform(referenceReplacer)).map(r => sp -> r)
            }.sequence
            computedAggregates <- aggregates.map { case (sa, exp) =>
              computeAggregate(exp.transform(referenceReplacer))
                .map(r => sa -> r)
            }.sequence
            groupByPartialGenerator = GroupByPartialGenerator(
              finalGenerator = GroupByPartialToFinalGenerator(
                stagedGroupBy = stagedGroupBy,
                computedAggregates = computedAggregates
              ),
              computedGroupingKeys = computedGroupingKeys,
              computedProjections = computedProjections
            )
            partialCFunction = groupByPartialGenerator.createPartial(inputs = inputsList)
            _ <-
              if (partialCFunction.outputs.toSet.size == partialCFunction.outputs.size) Right(())
              else
                Left(
                  s"Expected to have distinct outputs from a PF, got: ${partialCFunction.outputs}"
                )
            ff = groupByPartialGenerator.finalGenerator.createFinal
            fullFunction =
              groupByPartialGenerator.createFull(inputs = inputsList)
          } yield {
            options.preShufflePartitions match {
              case Some(n) =>
                ArrowColumnarToRowPlan(
                  NativeAggregationEvaluationPlan(
                    outputExpressions = aggregateExpressions,
                    functionPrefix = functionPrefix,
                    evaluationMode = EvaluationMode.PrePartitioned(fullFunction),
                    child = new RowToArrowColumnarPlan(
                      ShuffleExchangeExec(
                        outputPartitioning =
                          HashPartitioning(expressions = groupingExpressions, numPartitions = n),
                        child = planLater(child),
                        shuffleOrigin = REPARTITION
                      )
                    ),
                    supportsColumnar = true,
                    nativeEvaluator = nativeEvaluator
                  )
                )
              case None =>
                NativeAggregationEvaluationPlan(
                  outputExpressions = aggregateExpressions,
                  functionPrefix = functionPrefix,
                  evaluationMode = EvaluationMode.TwoStaged(partialCFunction, ff),
                  child = planLater(child),
                  supportsColumnar = false,
                  nativeEvaluator = nativeEvaluator
                )
            }
          }

          val evaluationPlan = evaluationPlanE.fold(sys.error, identity)
          logger.info(s"Plan is: ${evaluationPlan}")
          List(evaluationPlan)
        case Sort(orders, global, child) => {
          val inputsList = child.output.zipWithIndex.map { case (att, id) =>
            sparkTypeToScalarVeType(att.dataType)
              .makeCVector(s"${InputPrefix}${id}")
              .asInstanceOf[CScalarVector]
          }.toList

          implicit val fallback: EvalFallback = EvalFallback.noOp
          val orderingExpressions = orders
            .map { case SortOrder(child, direction, _, _) =>
              eval(replaceReferences(InputPrefix, plan.inputSet.toList, child))
                .map(elem =>
                  VeSortExpression(
                    TypedCExpression2(sparkTypeToScalarVeType(child.dataType), elem),
                    sparkSortDirectionToSortOrdering(direction)
                  )
                )
            }
            .toList
            .sequence
            .fold(
              expr =>
                sys.error(s"Failed to match expression ${expr}, with inputs ${plan.inputSet}"),
              identity
            )

          val veSort = VeSort(inputsList, orderingExpressions)
          val code = CFunctionGeneration.renderSort(veSort)
          val sortPlan = new NativeSortEvaluationPlan(
            outputExpressions = plan.output,
            functionPrefix = functionPrefix,
            Coalesced(code),
            new RowToArrowColumnarPlan(planLater(child)),
            nativeEvaluator = nativeEvaluator
          )
          List(new ArrowColumnarToRowPlan(sortPlan))
        }
        case _ => Nil
      }

      if (failFast) res
      else {
        try res
        catch {
          case e: Throwable =>
            logger.error(s"Could not map plan ${plan} because of: ${e}", e)
            Nil
        }
      }
    } else Nil
  }
}
