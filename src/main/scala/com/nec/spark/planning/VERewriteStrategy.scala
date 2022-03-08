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
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkExpressionToCExpression._
import com.nec.spark.agile.groupby.ConvertNamedExpression.{computeAggregate, mapGroupingExpression}
import com.nec.spark.agile.groupby.GroupByOutline.GroupingKey
import com.nec.spark.agile.groupby.{ConvertNamedExpression, GroupByOutline, GroupByPartialGenerator, GroupByPartialToFinalGenerator}
import com.nec.spark.agile.join.{GenericJoiner, JoinMatcher}
import com.nec.spark.agile.{CFunctionGeneration, SparkExpressionToCExpression, StringHole}
import com.nec.spark.planning.TransformUtil.RichTreeNode
import com.nec.spark.planning.VERewriteStrategy.{GroupPrefix, HashExchangeBuckets, InputPrefix, SequenceList}
import com.nec.spark.planning.VeFunction.VeFunctionStatus
import com.nec.spark.planning.aggregation.VeHashExchangePlan
import com.nec.spark.planning.hints._
import com.nec.spark.planning.plans._
import com.nec.ve.{FilterFunction, GroupingFunction, MergerFunction, ProjectionFunction, SortFunction}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Sort}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, Strategy}

object VERewriteStrategy {
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

  val HashExchangeBuckets: Int = SparkCycloneExecutorPlugin.totalVeCores()
}

final case class VERewriteStrategy(options: VeRewriteStrategyOptions)
  extends Strategy
  with LazyLogging {

  import com.github.ghik.silencer.silent

  @silent
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val functionPrefix: String = s"eval_${Math.abs(plan.toString.hashCode())}"

    if (options.rewriteEnabled) {
      log.debug(
        s"Processing input plan with VERewriteStrategy: $plan, output types were: ${plan.output
          .map(_.dataType)}; options = ${options}"
      )

      if (options.failFast) {
        val x = rewritePlan(functionPrefix, plan)
        if (x.isEmpty) logger.debug(s"Didn't rewrite") else logger.debug(s"Rewrote it to ==> ${x}")
        x
      } else {
        try rewritePlan(functionPrefix, plan)
        catch {
          case e: Throwable =>
            logger.error(s"Could not map plan ${plan} because of: ${e}", e)
            /* https://github.com/XpressAI/SparkCyclone/pull/407:
             InMemoryRelation is not being picked up in the translation process after a failure in plan transformation.
             Capture specific Filter+InMemory combination when such a failure happens. */
            plan match {
              case logical.Filter(cond, imr @ InMemoryRelation(_, cb, _))
                  if cb.serializer.isInstanceOf[CycloneCacheBase] =>
                inMemoryFilterPlan(cond, imr)

              case _ => Nil
            }
        }
      }
    } else Nil
  }

  private def rewritePlan(functionPrefix: String, plan: LogicalPlan) = plan match {
    /* Apply Hints */
    case SortOnVe(child, enabled) =>
      rewriteWithOptions(child, options.copy(enableVeSorting = enabled))
    case ProjectOnVe(child, enabled) =>
      rewriteWithOptions(child, options.copy(projectOnVe = enabled))
    case FilterOnVe(child, enabled) => rewriteWithOptions(child, options.copy(filterOnVe = enabled))
    case AggregateOnVe(child, enabled) =>
      rewriteWithOptions(child, options.copy(aggregateOnVe = enabled))
    case ExchangeOnVe(child, enabled) =>
      rewriteWithOptions(child, options.copy(exchangeOnVe = enabled))
    case FailFast(child, enabled) => rewriteWithOptions(child, options.copy(failFast = enabled))
    case JoinOnVe(child, enabled) => rewriteWithOptions(child, options.copy(joinOnVe = enabled))
    case AmplifyBatches(child, enabled) =>
      rewriteWithOptions(child, options.copy(amplifyBatches = enabled))
    case SkipVe(child, enabled) => rewriteWithOptions(child, options.copy(rewriteEnabled = enabled))

    /* Plan rewriting */
    case imr @ InMemoryRelation(_, cb, _) if cb.serializer.isInstanceOf[CycloneCacheBase] =>
      fetchFromCachePlan(imr)

    case logical.Filter(cond, imr @ InMemoryRelation(_, cb, _))
        if cb.serializer.isInstanceOf[CycloneCacheBase] =>
      inMemoryFilterPlan(cond, imr)

    case f: logical.Filter if options.filterOnVe =>
      filterPlan(functionPrefix, f)

    case JoinMatcher(JoinMatcher(leftChild, rightChild, inputsLeft, inputsRight, genericJoiner))
        if options.joinOnVe =>
      joinPlan(functionPrefix, leftChild, rightChild, inputsLeft, inputsRight, genericJoiner)

    case logical.Project(projectList, child) if projectList.nonEmpty && options.projectOnVe =>
      projectPlan(functionPrefix, plan, projectList, child)

    case logical.Aggregate(groupingExpressions, aggregateExpressions, child)
      if options.aggregateOnVe && child.output.nonEmpty && aggregateExpressions.nonEmpty &&
        isSupportedAggregationExpression(aggregateExpressions) =>
      aggregatePlan(functionPrefix, groupingExpressions, aggregateExpressions, child)

    case s @ Sort(orders, _, child) if options.enableVeSorting && isSupportedSortType(child) =>
      sortPlan(functionPrefix, plan, s, orders, child)

    case _ => Nil
  }

  /**
   * String is not supported by Sort yet
   */
  private def isSupportedSortType(child: LogicalPlan) =
    !child.output.map(_.dataType).toSet.contains(StringType)

  private def isSupportedAggregationExpression(aggregateExpressions: Seq[NamedExpression]) = {
    !aggregateExpressions
      .collect {
        case ae: AggregateExpression           => ae
        case Alias(ae: AggregateExpression, _) => ae
      }
      .exists { ae =>
        /** HyperLogLog++ or Distinct not supported * */
        ae.aggregateFunction.isInstanceOf[HyperLogLogPlusPlus] || ae.isDistinct
      }
  }

  private def sortPlan(
    functionPrefix: String,
    plan: LogicalPlan,
    s: Sort,
    orders: Seq[SortOrder],
    child: LogicalPlan
  ) = {
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
        expr => sys.error(s"Failed to match expression ${expr}, with inputs ${plan.inputSet}"),
        identity
      )

    val veSort = VeSort(inputsList, orderingExpressions)
    val code = CFunctionGeneration.renderSort(veSort)
    val sortFName = s"sort_${functionPrefix}"

    val sortFn = SortFunction(
      s"sort_${functionPrefix}",
      inputsList,
      orderingExpressions
    )

    List(
      VectorEngineToSparkPlan(
        VeOneStageEvaluationPlan(
          outputExpressions = s.output,
          veFunction = VeFunction(
            veFunctionStatus = VeFunctionStatus.fromCodeLines(sortFn.toCodeLines /* code.toCodeLinesSPtr(sortFName) */),
            functionName = sortFn.name /* sortFName */,
            namedResults = sortFn.outputs /* code.outputs */
          ),
          child = SparkToVectorEnginePlan(planLater(child))
        )
      )
    )
  }


  private def aggregatePlan(functionPrefix: String, groupingExpressions: Seq[Expression], aggregateExpressions: Seq[NamedExpression], child: LogicalPlan) = {
    implicit val fallback: EvalFallback = EvalFallback.noOp

    val groupingExpressionsKeys: List[(GroupingKey, Expression)] =
      groupingExpressions.zipWithIndex.map { case (e, i) =>
        GroupingKey(
          name = s"${GroupPrefix}${i}",
          veType = SparkExpressionToCExpression.sparkTypeToVeType(e.dataType)
        ) -> e
      }.toList

    val referenceReplacer =
      SparkExpressionToCExpression.referenceReplacer(
        prefix = InputPrefix,
        inputs = child.output.toList
      )

    val inputsList = getInputAsVeTypes(child)

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
              case (agg, `namedExp`)           => Right(agg)
              case (agg, Alias(`namedExp`, _)) => Right(agg)
            }
          }
          .toRight(
            s"Unmatched output: ${namedExp}; type ${namedExp.getClass}; Spark type ${namedExp.dataType}. Have aggregates: ${aggregates
              .mkString(",")}"
          )
      }

      finalOutputs <- stringHoledAggregateExpressions.map(validateNamedOutput).toList.sequence

      stagedGroupBy = GroupByOutline(
        groupingKeys = groupingExpressionsKeys.map { case (gk, _) => gk },
        finalOutputs = finalOutputs
      )

      computedGroupingKeys <- groupingExpressionsKeys.map { case (gk, exp) =>
        mapGroupingExpression(exp, referenceReplacer).map(gk -> _)
      }.sequence

      stringHoles = projections.flatMap { case (_, p) =>
        StringHole.process(p.transform(referenceReplacer))
      } ++ aggregates.flatMap { case (_, exp) =>
        StringHole.process(exp.transform(referenceReplacer))
      }

      computedProjections <- projections.map { case (sp, p) =>
        ConvertNamedExpression
          .doProj(p.transform(referenceReplacer).transform(StringHole.transform))
          .map(sp -> _)
      }.sequence

      computedAggregates <- aggregates.map { case (sa, exp) =>
        computeAggregate(exp.transform(referenceReplacer)).map(sa -> _)
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
        else Left(s"Expected to have distinct outputs from a PF, got: ${partialCFunction.outputs}")

      ff = groupByPartialGenerator.finalGenerator.createFinal

      dataDescriptions = {
        child.output.map { expx =>
          val contained =
            groupingExpressions.exists(exp =>
              exp == expx || exp.collect { case `expx` => exp }.nonEmpty
            ) ||
              groupingExpressions
                .collect { case ar: AttributeReference => ar.exprId }
                .toSet
                .contains(expx.exprId)

          GroupingFunction.DataDescription(
            sparkTypeToVeType(expx.dataType),
            if (contained) GroupingFunction.Key else GroupingFunction.Value
          )
        }
      }

      partialName = s"partial_$functionPrefix"
      finalName = s"final_$functionPrefix"

      inputAmplifyFn = MergerFunction(s"input_amplify_${functionPrefix}", partialCFunction.inputs.map(_.veType))
      //exchangeFunction = GroupingFunction(s"exchange_${functionPrefix}", dataDescriptions.toList, HashExchangeBuckets)
      mergeFn = MergerFunction(s"merge_$functionPrefix", partialCFunction.outputs.map(_.veType))
      amplifyFn = MergerFunction(s"amplify_${functionPrefix}", ff.outputs.map(_.veType))

      code = CodeLines
        .from(
          partialCFunction.toCodeLinesHeaderBatchPtr(partialName),
          ff.toCodeLinesNoHeaderOutPtr2(finalName),
          //exchangeFunction.toCodeLines,
          inputAmplifyFn.toCodeLines,
          mergeFn.toCodeLines,
          if (options.amplifyBatches) amplifyFn.toCodeLines else CodeLines.empty
        )

    } yield {
      val exchangePlan = VeAmplifyBatchesPlan(
        amplifyFunction = VeFunction(
          veFunctionStatus = VeFunctionStatus.fromCodeLines(code),
          functionName = inputAmplifyFn.name,
          namedResults = inputAmplifyFn.outputs
        ),
        child = SparkToVectorEnginePlan(planLater(child))
      )

      val pag = VePartialAggregate(
        partialFunction = VeFunction(
          veFunctionStatus = VeFunctionStatus.fromCodeLines(code),
          functionName = partialName,
          namedResults = partialCFunction.outputs
        ),
        child = exchangePlan,
        expectedOutputs = partialCFunction.outputs
          .map(_.veType)
          .zipWithIndex
          .map { case (veType, i) =>
            import org.apache.spark.sql.catalyst.expressions._
            // quick hack before doing something more proper
            AttributeReference(
              s"${veType}_${i}",
              SparkExpressionToCExpression.likelySparkType(veType)
            )()
          }
      )

      val flt = VeFlattenPartition(
        flattenFunction = VeFunction(
          veFunctionStatus = VeFunctionStatus.fromCodeLines(code),
          functionName = mergeFn.name,
          namedResults = partialCFunction.outputs
        ),
        child = pag
      )

      val finalAggregate = VeFinalAggregate(
        expectedOutputs = aggregateExpressions,
        finalFunction = VeFunction(
          veFunctionStatus = VeFunctionStatus.fromCodeLines(code),
          functionName = finalName,
          namedResults = ff.outputs
        ),
        child = flt
      )

      VectorEngineToSparkPlan(
        if (options.amplifyBatches)
          VeAmplifyBatchesPlan(
            amplifyFunction = VeFunction(
              veFunctionStatus = VeFunctionStatus.fromCodeLines(code),
              functionName = amplifyFn.name,
              namedResults = ff.outputs
            ),
            child = finalAggregate
          )
        else
          finalAggregate
      )
    }

    val evaluationPlan = evaluationPlanE.fold(sys.error, identity)
    List(evaluationPlan)
  }

  private def projectPlan(
    functionPrefix: String,
    plan: LogicalPlan,
    projectList: Seq[NamedExpression],
    child: LogicalPlan
  ) = {
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
            Left(NamedStringExpression(name = s"output_${idx}", stringProducer = stringProducer))
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
      val projectionFn = ProjectionFunction(
        s"project_${functionPrefix}",
        getInputAsVeTypes(child),
        outputs
      )

      List(VectorEngineToSparkPlan(if (options.passThroughProject) {
        VeProjectEvaluationPlan(
          outputExpressions = projectList,
          veFunction = VeFunction(
            veFunctionStatus = VeFunctionStatus.fromCodeLines(projectionFn.toCodeLines),
            functionName = projectionFn.name,
            namedResults = projectionFn.outputs
          ),
          child = SparkToVectorEnginePlan(planLater(child))
        )
      } else {
        VeOneStageEvaluationPlan(
          outputExpressions = projectList,
          veFunction = VeFunction(
            veFunctionStatus = VeFunctionStatus.fromCodeLines(projectionFn.toCodeLines),
            functionName = projectionFn.name,
            namedResults = projectionFn.outputs
          ),
          child = SparkToVectorEnginePlan(planLater(child))
        )
      }))
    }

    planE.fold(e => sys.error(s"Could not map ${e}"), identity)
  }

  private def inMemoryFilterPlan(cond: Expression, imr: InMemoryRelation) = {
    SparkSession.active.sessionState.planner.InMemoryScans
      .apply(imr)
      .flatMap(sp =>
        List(
          FilterExec(
            cond,
            VectorEngineToSparkPlan(
              VeFetchFromCachePlan(sp, imr.cacheBuilder.serializer.asInstanceOf[CycloneCacheBase])
            )
          )
        )
      )
      .toList
  }

  private def filterPlan(functionPrefix: String, f: Filter) = {
    implicit val fallback: EvalFallback = EvalFallback.noOp

    val child = f.child
    val condition = f.condition

    val replacer =
      SparkExpressionToCExpression.referenceReplacer(InputPrefix, child.output.toList)
    val planE = for {
      cond <- eval(condition.transform(replacer).transform(StringHole.transform))
      data = getInputAsVeTypes(child)
    } yield {
      val filterFn = FilterFunction(
        s"filter_${functionPrefix}",
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

      val amplifyFn = MergerFunction(s"amplify_${filterFn.name}", data.map(_.veType))

      val filterPlan = VeOneStageEvaluationPlan(
        outputExpressions = f.output,
        veFunction = VeFunction(
          veFunctionStatus = VeFunctionStatus.fromCodeLines(filterFn.toCodeLines),
          functionName = filterFn.name,
          namedResults = filterFn.outputs
        ),
        child = SparkToVectorEnginePlan(planLater(child))
      )

      List(
        VectorEngineToSparkPlan(
          if (options.amplifyBatches)
            VeAmplifyBatchesPlan(
              amplifyFunction = VeFunction(
                veFunctionStatus = VeFunctionStatus.fromCodeLines(
                  CodeLines.from(CFunctionGeneration.KeyHeaders, amplifyFn.toCodeLines)
                ),
                functionName = amplifyFn.name,
                namedResults = data
              ),
              child = filterPlan
            )
          else filterPlan
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
  }

  private def joinPlan(
    functionPrefix: String,
    leftChild: LogicalPlan,
    rightChild: LogicalPlan,
    inputsLeft: List[CVector],
    inputsRight: List[CVector],
    genericJoiner: GenericJoiner
  ) = {
    val functionName = s"join_${functionPrefix}"

    val leftExchangePlan = getJoinExchangePlan(s"l_$functionPrefix", leftChild, inputsLeft, genericJoiner)
    val rightExchangePlan = getJoinExchangePlan(s"r_$functionPrefix", rightChild, inputsRight, genericJoiner)

    val joinFunction = VeFunction(
        veFunctionStatus = {
          val produceIndicesFName = s"produce_indices_${functionName}"
          VeFunctionStatus.fromCodeLines(CodeLines.from(
                CFunctionGeneration.KeyHeaders,
                genericJoiner.cFunctionExtra.toCodeLinesNoHeader(produceIndicesFName),
                genericJoiner
                  .cFunction(produceIndicesFName)
                  .toCodeLines(functionName)
              )
          )
      },
      functionName = functionName,
      namedResults = genericJoiner.outputs.map(_.cVector)
    )
    val joinPlan = VectorEngineJoinPlan(
      outputExpressions = leftChild.output ++ rightChild.output,
      joinFunction = joinFunction,
      left = leftExchangePlan,
      right = rightExchangePlan
    )

    val amplifyFn =
      MergerFunction(s"amplify_${functionName}", genericJoiner.outputs.map(_.cVector.veType))

    val maybeAmplifiedJoinPlan =
      if (options.amplifyBatches)
        VeAmplifyBatchesPlan(
          amplifyFunction = VeFunction(
            veFunctionStatus = VeFunctionStatus.fromCodeLines(
              CodeLines.from(CFunctionGeneration.KeyHeaders, amplifyFn.toCodeLines)
            ),
            functionName = amplifyFn.name,
            namedResults = genericJoiner.outputs.map(_.cVector)
          ),
          child = joinPlan
        )
      else joinPlan

    List(VectorEngineToSparkPlan(maybeAmplifiedJoinPlan))
  }

  private def getJoinExchangePlan(
    fnName: String,
    child: LogicalPlan,
    inputs: List[CVector],
    genericJoiner: GenericJoiner
  ) = {
    val exchangeFunctionL = GroupingFunction(
      fnName,
      inputs.map { vec =>
        GroupingFunction.DataDescription(
          vec.veType,
          if (genericJoiner.joins.flatMap(_.vecs).contains(vec)) GroupingFunction.Key
          else GroupingFunction.Value
        )
      },
      HashExchangeBuckets
    )

    val code = CodeLines.from(CFunctionGeneration.KeyHeaders, exchangeFunctionL.toCodeLines)

    VeHashExchangePlan(
      exchangeFunction = VeFunction(
        veFunctionStatus = VeFunctionStatus.fromCodeLines(code),
        functionName = exchangeFunctionL.name,
        namedResults = inputs
      ),
      child = SparkToVectorEnginePlan(planLater(child))
    )
  }

  private def fetchFromCachePlan(imr: InMemoryRelation) = {
    SparkSession.active.sessionState.planner.InMemoryScans
      .apply(imr)
      .flatMap(sp =>
        List(
          VectorEngineToSparkPlan(
            VeFetchFromCachePlan(sp, imr.cacheBuilder.serializer.asInstanceOf[CycloneCacheBase])
          )
        )
      )
      .toList
  }

  private def rewriteWithOptions(child: LogicalPlan, options: VeRewriteStrategyOptions) = {
    val ret = VERewriteStrategy(options).apply(child)
    Seq(ret: _*)
  }

  private def getInputAsVeTypes(child: LogicalPlan) = child.output.toList.zipWithIndex.map {
    case (att, idx) =>
      sparkTypeToVeType(att.dataType).makeCVector(s"${InputPrefix}$idx")
  }
}
