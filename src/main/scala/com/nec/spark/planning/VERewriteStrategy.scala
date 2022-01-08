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

import com.nec.cache.CycloneCacheBase
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression._
import com.nec.spark.agile.groupby.ConvertNamedExpression.{computeAggregate, mapGroupingExpression}
import com.nec.spark.agile.groupby.GroupByOutline.GroupingKey
import com.nec.spark.agile.groupby.{ConvertNamedExpression, GroupByOutline, GroupByPartialGenerator, GroupByPartialToFinalGenerator}
import com.nec.spark.agile.{CFunctionGeneration, SparkExpressionToCExpression, StringHole}
import com.nec.spark.planning.TransformUtil.RichTreeNode
import com.nec.spark.planning.VERewriteStrategy.{GroupPrefix, InputPrefix, SequenceList}
import com.nec.spark.planning.VeFunction.VeFunctionStatus
import com.nec.spark.planning.aggregation.VeHashExchange
import com.nec.spark.planning.plans._
import com.nec.ve.GroupingFunction.DataDescription
import com.nec.ve.{GroupingFunction, MergerFunction}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.exchange.{REPARTITION, ShuffleExchangeExec}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, Strategy}

import scala.collection.immutable
import scala.util.Try

object VERewriteStrategy {
  var _enabled: Boolean = true
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
  options: VeRewriteStrategyOptions = VeRewriteStrategyOptions.default
) extends Strategy
  with LazyLogging {

  import com.github.ghik.silencer.silent

  @silent
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def functionPrefix: String = s"eval_${Math.abs(plan.toString.hashCode())}"

    if (VERewriteStrategy._enabled) {
      log.debug(
        s"Processing input plan with VERewriteStrategy: $plan, output types were: ${plan.output
          .map(_.dataType)}; options = ${options}"
      )

      def res: immutable.Seq[SparkPlan] = plan match {
        case imr @ InMemoryRelation(_, cb, oo)
            if cb.serializer
              .isInstanceOf[CycloneCacheBase] =>
          SparkSession.active.sessionState.planner.InMemoryScans
            .apply(imr)
            .flatMap(sp => List(VectorEngineToSparkPlan(VeFetchFromCachePlan(sp))))
            .toList

        case f @ logical.Filter(condition, child) if options.filterOnVe =>
          implicit val fallback: EvalFallback = EvalFallback.noOp

          val replacer =
            SparkExpressionToCExpression.referenceReplacer(InputPrefix, child.output.toList)
          val planE = for {
            cond <- eval(condition.transform(replacer).transform(StringHole.transform))
            data = child.output.toList.zipWithIndex.map { case (att, idx) =>
              sparkTypeToVeType(att.dataType).makeCVector(s"${InputPrefix}$idx")
            }
          } yield {
            val functionName = s"flt_${functionPrefix}"
            val cFunction = renderFilter(
              VeFilter(
                stringVectorComputations = StringHole
                  .process(condition.transform(replacer))
                  .toList
                  .flatMap(_.stringParts)
                  .distinct,
                data = data,
                condition = cond
              )
            )

            List(
              VectorEngineToSparkPlan(
                OneStageEvaluationPlan(
                  outputExpressions = f.output,
                  veFunction = VeFunction(
                    veFunctionStatus =
                      VeFunctionStatus.SourceCode(cFunction.toCodeLinesSPtr(functionName).cCode),
                    functionName = functionName,
                    results = cFunction.outputs.map(_.veType)
                  ),
                  child = SparkToVectorEnginePlan(planLater(child))
                )
              )
            )
          }

          planE.fold(
            e =>
              sys.error(
                s"Could not map ${e} (${e.getClass} ${Option(e)
                  .collect { case a: AttributeReference => a.dataType }}); input is ${child.output.toList}, condition is ${condition}"
              ),
            identity
          )
        case f @ logical.Filter(cond, imr @ InMemoryRelation(output, cb, oo))
            if cb.serializer
              .isInstanceOf[CycloneCacheBase] =>
          SparkSession.active.sessionState.planner.InMemoryScans
            .apply(imr)
            .flatMap(sp =>
              List(FilterExec(cond, VectorEngineToSparkPlan(VeFetchFromCachePlan(sp))))
            )
            .toList

        case logical.Project(projectList, child) if projectList.nonEmpty && options.projectOnVe =>
          implicit val fallback: EvalFallback = EvalFallback.noOp
          val nonIdentityProjections =
            if (options.passThroughProject)
              projectList.filter(_.isInstanceOf[AttributeReference])
            else projectList

          val planE = for {
            outputs <- nonIdentityProjections.toList.zipWithIndex.map { case (att, idx) =>
              val referenced = replaceReferences(InputPrefix, plan.inputSet.toList, att)
              if (referenced.dataType == StringType)
                evalString(referenced).map(stringProducer =>
                  Left(
                    NamedStringExpression(name = s"output_${idx}", stringProducer = stringProducer)
                  )
                )
              else
                eval(referenced).map { cexp =>
                  Right(
                    NamedTypedCExpression(
                      name = s"output_${idx}",
                      sparkTypeToScalarVeType(att.dataType),
                      cExpression = cexp
                    )
                  )
                }
            }.sequence
          } yield {
            val fName = s"project_${functionPrefix}"
            val cF = renderProjection(
              VeProjection(
                inputs = child.output.toList.zipWithIndex.map { case (att, idx) =>
                  sparkTypeToVeType(att.dataType).makeCVector(s"${InputPrefix}${idx}")
                },
                outputs = outputs
              )
            )
            List(VectorEngineToSparkPlan(if (options.passThroughProject) {
              ProjectEvaluationPlan(
                outputExpressions = projectList,
                veFunction = VeFunction(
                  veFunctionStatus = VeFunctionStatus.SourceCode(cF.toCodeLinesSPtr(fName).cCode),
                  functionName = fName,
                  results = cF.outputs.map(_.veType)
                ),
                child = SparkToVectorEnginePlan(planLater(child))
              )
            } else {
              OneStageEvaluationPlan(
                outputExpressions = projectList,
                veFunction = VeFunction(
                  veFunctionStatus = VeFunctionStatus.SourceCode(cF.toCodeLinesSPtr(fName).cCode),
                  functionName = fName,
                  results = cF.outputs.map(_.veType)
                ),
                child = SparkToVectorEnginePlan(planLater(child))
              )
            }))
          }

          planE.fold(e => sys.error(s"Could not map ${e}"), identity)

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

          val stringHoledAggregateExpressions = aggregateExpressions.map(namedExpression =>
            namedExpression.transformSelf(referenceReplacer).transformSelf(StringHole.transform)
          )

          val evaluationPlanE: Either[String, SparkPlan] = for {
            projections <- stringHoledAggregateExpressions.zipWithIndex
              .map { case (ne, i) =>
                ConvertNamedExpression
                  .mapNamedExp(
                    namedExpression = ne,
                    idx = i,
                    referenceReplacer = referenceReplacer,
                    childAttributes = child.output
                  )
                  .map(_.right.toSeq)
              }
              .toList
              .sequence
              .map(_.flatten)
            aggregates <-
              stringHoledAggregateExpressions.zipWithIndex
                .map { case (ne, i) =>
                  ConvertNamedExpression
                    .mapNamedExp(
                      namedExpression = ne,
                      idx = i,
                      referenceReplacer = referenceReplacer,
                      childAttributes = child.output
                    )
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
            finalOutputs <- stringHoledAggregateExpressions
              .map(validateNamedOutput)
              .toList
              .sequence
            stagedGroupBy = GroupByOutline(
              groupingKeys = groupingExpressionsKeys.map { case (gk, _) => gk },
              finalOutputs = finalOutputs
            )
            computedGroupingKeys <-
              groupingExpressionsKeys.map { case (gk, exp) =>
                mapGroupingExpression(exp, referenceReplacer)
                  .map(e => gk -> e)
              }.sequence
            stringHoles = projections.flatMap { case (sp, p) =>
              StringHole.process(p.transform(referenceReplacer))
            } ++ aggregates.flatMap { case (sa, exp) =>
              StringHole.process(exp.transform(referenceReplacer))
            }
            computedProjections <- projections.map { case (sp, p) =>
              ConvertNamedExpression
                .doProj(p.transform(referenceReplacer).transform(StringHole.transform))
                .map(r => sp -> r)
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
              computedProjections = computedProjections,
              stringVectorComputations = stringHoles.flatMap(_.stringParts).distinct
            )
            partialCFunction = groupByPartialGenerator.createPartial(inputs = inputsList)
            _ <-
              if (partialCFunction.outputs.toSet.size == partialCFunction.outputs.size) Right(())
              else
                Left(
                  s"Expected to have distinct outputs from a PF, got: ${partialCFunction.outputs}"
                )
            ff = groupByPartialGenerator.finalGenerator.createFinal
            partialName = s"partial_$functionPrefix"
            finalName = s"final_$functionPrefix"
            exchangeName = s"exchange_$functionPrefix"
            exchangeFunction = {
              val gd = child.output.map { expx =>
                val contained =
                  groupingExpressions.exists(exp =>
                    exp == expx || exp.collect { case `expx` => exp }.nonEmpty
                  ) ||
                    groupingExpressions
                      .collect { case ar: AttributeReference => ar.exprId }
                      .toSet
                      .contains(expx.exprId)

                DataDescription(
                  veType = sparkTypeToVeType(expx.dataType),
                  keyOrValue =
                    if (contained) DataDescription.KeyOrValue.Key
                    else DataDescription.KeyOrValue.Value
                )
              }.toList
              GroupingFunction.groupData(data = gd, totalBuckets = 16)
            }
            mergeFunction = s"merge_$functionPrefix"
            code = CodeLines
              .from(
                partialCFunction.toCodeLinesSPtr(partialName),
                ff.toCodeLinesNoHeaderOutPtr2(finalName),
                exchangeFunction.toCodeLines(exchangeName),
                MergerFunction
                  .merge(types = partialCFunction.outputs.map(_.veType))
                  .toCodeLines(mergeFunction)
                  .cCode
              )

          } yield {

            val exchangePlan =
              if (options.exchangeOnVe)
                VeHashExchange(
                  exchangeFunction = VeFunction(
                    veFunctionStatus = VeFunctionStatus.SourceCode(code.cCode),
                    functionName = exchangeName,
                    results = partialCFunction.inputs.map(_.veType)
                  ),
                  child = SparkToVectorEnginePlan(planLater(child))
                )
              else
                SparkToVectorEnginePlan(
                  ShuffleExchangeExec(
                    outputPartitioning =
                      HashPartitioning(expressions = groupingExpressions, numPartitions = 8),
                    child = planLater(child),
                    shuffleOrigin = REPARTITION
                  )
                )
            val pag = VePartialAggregate(
              partialFunction = VeFunction(
                veFunctionStatus = VeFunctionStatus.SourceCode(code.cCode),
                functionName = partialName,
                results = partialCFunction.outputs.map(_.veType)
              ),
              child = exchangePlan,
              expectedOutputs = partialCFunction.outputs
                .map(_.veType)
                .zipWithIndex
                .map { case (veType, i) =>
                  import org.apache.spark.sql.catalyst.expressions._
                  // quick hack before doing something more proper
                  PrettyAttribute(
                    s"${veType}_${i}",
                    SparkExpressionToCExpression.likelySparkType(veType)
                  )
                }
            )
            val flt = VeFlattenPartition(
              flattenFunction = VeFunction(
                veFunctionStatus = VeFunctionStatus.SourceCode(code.cCode),
                functionName = mergeFunction,
                results = partialCFunction.outputs.map(_.veType)
              ),
              child = pag
            )
            VectorEngineToSparkPlan(
              VeFinalAggregate(
                expectedOutputs = aggregateExpressions,
                finalFunction = VeFunction(
                  veFunctionStatus = VeFunctionStatus.SourceCode(code.cCode),
                  functionName = finalName,
                  results = ff.outputs.map(_.veType)
                ),
                child = flt
              )
            )
          }

          val evaluationPlan = evaluationPlanE.fold(sys.error, identity)
          List(evaluationPlan)
        case s @ Sort(orders, global, child)
            if options.enableVeSorting && !child.output
              .map(_.dataType)
              .toSet
              .contains(StringType) /* String is not supported by Sort yet */ => {
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

          val sortFName = s"sort_${functionPrefix}"

          List(
            VectorEngineToSparkPlan(
              OneStageEvaluationPlan(
                outputExpressions = s.output,
                veFunction = VeFunction(
                  veFunctionStatus =
                    VeFunctionStatus.SourceCode(code.toCodeLinesSPtr(sortFName).cCode),
                  functionName = sortFName,
                  results = code.outputs.map(_.veType)
                ),
                child = SparkToVectorEnginePlan(planLater(child))
              )
            )
          )
        }
        case _ => Nil
      }

      if (options.failFast) {
        val x = res
        if (x.isEmpty)
          logger.debug(s"Didn't rewrite")
        else
          logger.debug(s"Rewrote it to ==> ${x}")
        x
      } else {
        try res
        catch {
          case e: Throwable =>
            logger.error(s"Could not map plan ${plan} because of: ${e}", e)
            plan match {
              case f @ logical.Filter(cond, imr @ InMemoryRelation(output, cb, oo))
                  if cb.serializer
                    .isInstanceOf[CycloneCacheBase] =>
                SparkSession.active.sessionState.planner.InMemoryScans
                  .apply(imr)
                  .flatMap(sp =>
                    List(FilterExec(cond, VectorEngineToSparkPlan(VeFetchFromCachePlan(sp))))
                  )
                  .toList
              case _ => Nil
            }
        }
      }
    } else Nil
  }
}
