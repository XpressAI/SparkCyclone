package com.nec.spark.planning

import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkVeMapper.EvaluationAttempt._
import com.nec.spark.agile.SparkVeMapper.{sparkTypeToVeType, EvalFallback}
import com.nec.spark.agile.groupby.GroupByOutline.{
  GroupingKey,
  StagedAggregation,
  StagedAggregationAttribute,
  StagedProjection,
  StringReference
}
import com.nec.spark.agile.groupby.{
  GroupByOutline,
  GroupByPartialGenerator,
  GroupByPartialToFinalGenerator
}
import com.nec.spark.agile.{DeclarativeAggregationConverter, SparkVeMapper}
import com.nec.spark.planning.NativeAggregationEvaluationPlan.EvaluationMode
import com.nec.spark.planning.VERewriteStrategy.{SequenceList, VeRewriteStrategyOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate,
  HyperLogLogPlusPlus
}
import org.apache.spark.sql.catalyst.expressions.{aggregate, Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{REPARTITION, ShuffleExchangeExec}
import org.apache.spark.sql.types.StringType

import scala.collection.immutable
import scala.util.Try

object VERewriteStrategy {
  var _enabled: Boolean = true
  var failFast: Boolean = false
  final case class VeRewriteStrategyOptions(preShufflePartitions: Option[Int])
  object VeRewriteStrategyOptions {
    val default: VeRewriteStrategyOptions = VeRewriteStrategyOptions(preShufflePartitions = Some(8))
  }

  implicit class SequenceList[A, B](l: List[Either[A, B]]) {
    def sequence: Either[A, List[B]] = l.flatMap(_.left.toOption).headOption match {
      case Some(error) => Left(error)
      case None        => Right(l.flatMap(_.right.toOption))
    }
  }
}

final case class VERewriteStrategy(
  nativeEvaluator: NativeEvaluator,
  options: VeRewriteStrategyOptions = VeRewriteStrategyOptions.default
) extends Strategy
  with LazyLogging {

  import com.github.ghik.silencer.silent

  @silent
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def fName: String = s"eval_${Math.abs(plan.toString.hashCode())}"

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
                  name = s"group_${i}",
                  veType = SparkVeMapper.sparkTypeToVeType(e.dataType)
                ),
                e
              )
            }.toList

          val projections: List[(StagedProjection, Expression)] =
            aggregateExpressions.zipWithIndex
              .collect {
                case (ar: AttributeReference, idx)
                    if child.output.toList.exists(_.exprId == ar.exprId) =>
                  Option(StagedProjection(s"sp_${idx}", sparkTypeToVeType(ar.dataType)) -> ar)
                case (anything, idx)
                    if anything.isInstanceOf[aggregate.AggregateExpression] || anything
                      .find(_.isInstanceOf[DeclarativeAggregate])
                      .nonEmpty =>
                  /** Intentionally ignore -- these are not actual projections */
                  Option.empty
                case (Alias(exp, name), idx) =>
                  Option(StagedProjection(s"sp_${idx}", sparkTypeToVeType(exp.dataType)) -> exp)
                case other =>
                  sys.error(s"Unexpected aggregate expression: ${other}, type ${other._1.getClass}")
              }
              .toList
              .flatten

          val aggregates: List[(StagedAggregation, Expression)] =
            aggregateExpressions.zipWithIndex
              .collect {
                case (
                      o @ Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _),
                      idx
                    ) =>
                  (
                    GroupByExpression.GroupByAggregation(
                      DeclarativeAggregationConverter(
                        d.transform(SparkVeMapper.referenceReplacer("input_", child.output.toList))
                          .asInstanceOf[DeclarativeAggregate]
                      )
                    ),
                    o,
                    idx
                  )
                case (Alias(other, _), idx) if other.collectFirst { case _: DeclarativeAggregate =>
                      ()
                    }.nonEmpty =>
                  (
                    GroupByExpression.GroupByAggregation(
                      DeclarativeAggregationConverter
                        .transformingFetch(
                          other
                            .transform(
                              SparkVeMapper.referenceReplacer("input_", child.output.toList)
                            )
                        )
                        .getOrElse(
                          sys.error(
                            s"Cannot figure out how to replace: ${other} (${other.getClass})"
                          )
                        )
                    ),
                    other,
                    idx
                  )
                case (other, idx) if other.collectFirst { case _: DeclarativeAggregate =>
                      ()
                    }.nonEmpty =>
                  (
                    GroupByExpression.GroupByAggregation(
                      DeclarativeAggregationConverter
                        .transformingFetch(
                          other
                            .transform(
                              SparkVeMapper.referenceReplacer("input_", child.output.toList)
                            )
                        )
                        .getOrElse(
                          sys.error(
                            s"Cannot figure out how to replace: ${other} (${other.getClass})"
                          )
                        )
                    ),
                    other,
                    idx
                  )
              }
              .toList
              .map { case (groupByExpression, expr, idx) =>
                StagedAggregation(
                  s"agg_${idx}",
                  sparkTypeToVeType(expr.dataType),
                  groupByExpression.aggregation.partialValues(s"agg_${idx}").map { case (cs, ce) =>
                    StagedAggregationAttribute(name = cs.name, veScalarType = cs.veType)
                  }
                ) -> expr
              }

          val stagedGroupBy = GroupByOutline(
            groupingKeys = groupingExpressionsKeys.map { case (gk, e) => gk },
            finalOutputs = aggregateExpressions.map { namedExp_ =>
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
                .getOrElse(
                  sys.error(
                    s"Unmatched output: ${namedExp}; type ${namedExp.getClass}; Spark type ${namedExp.dataType}. Have aggregates: ${aggregates
                      .mkString(",")}"
                  )
                )
            }.toList
          )

          val computeGroupingKey
            : GroupingKey => Either[String, Either[StringReference, TypedCExpression2]] =
            gk =>
              groupingExpressionsKeys.toMap
                .get(gk)
                .toRight(s"Could not compute grouping key ${gk}")
                .flatMap(exp => mapGroupingExpression(exp, child))

          val computeProjection
            : StagedProjection => Either[String, Either[StringReference, TypedCExpression2]] =
            sp =>
              projections.toMap
                .get(sp)
                .toRight(s"Could not find a projection for ${sp}")
                .flatMap {
                  case ar: AttributeReference if ar.dataType == StringType =>
                    Right(
                      Left(
                        StringReference(
                          ar.transform(
                            SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                          ).asInstanceOf[AttributeReference]
                            .name
                        )
                      )
                    )
                  case Alias(ar: AttributeReference, _) if ar.dataType == StringType =>
                    Right(
                      Left(
                        StringReference(
                          ar.transform(
                            SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                          ).asInstanceOf[AttributeReference]
                            .name
                        )
                      )
                    )
                  case Alias(other, _) =>
                    SparkVeMapper
                      .eval(
                        other.transform(
                          SparkVeMapper
                            .referenceReplacer(prefix = "input_", child.output.toList)
                        )
                      )
                      .reportToString
                      .map(ce =>
                        Right(
                          TypedCExpression2(
                            SparkVeMapper.sparkTypeToScalarVeType(other.dataType),
                            ce
                          )
                        )
                      )
                  case other =>
                    SparkVeMapper
                      .eval(
                        other.transform(
                          SparkVeMapper
                            .referenceReplacer(prefix = "input_", child.output.toList)
                        )
                      )
                      .reportToString
                      .map(ce =>
                        Right(
                          TypedCExpression2(
                            SparkVeMapper.sparkTypeToScalarVeType(other.dataType),
                            ce
                          )
                        )
                      )
                }

          val computeAggregate: StagedAggregation => Either[String, Aggregation] = sa =>
            aggregates.toMap
              .get(sa)
              .map {
                case Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _) =>
                  DeclarativeAggregationConverter(
                    d.transform(
                      SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                    ).asInstanceOf[DeclarativeAggregate]
                  )
                case other =>
                  DeclarativeAggregationConverter
                    .transformingFetch(
                      other
                        .transform(
                          SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                        )
                    )
                    .getOrElse(
                      sys.error(s"Cannot figure out how to replace: ${other} (${other.getClass})")
                    )
              }
              .toRight(s"Could not compute aggregate for ${sa}")

          logInfo(s"Staged groupBy = ${stagedGroupBy}")

          val inputsList = child.output.zipWithIndex.map { case (att, id) =>
            sparkTypeToVeType(att.dataType).makeCVector(s"input_${id}")
          }.toList

          val pf = {
            for {
              gks <-
                stagedGroupBy.groupingKeys
                  .map(gk => computeGroupingKey(gk).map(r => gk -> r))
                  .sequence
              ps <- projections.map { case (sp, _) =>
                computeProjection(sp).map(r => sp -> r)
              }.sequence
              ca <- stagedGroupBy.aggregations
                .map(sa => computeAggregate(sa).map(a => sa -> a))
                .sequence
            } yield GroupByPartialGenerator(
              finalGenerator = GroupByPartialToFinalGenerator(
                stagedGroupBy = stagedGroupBy,
                computeAggregate = ca
              ),
              computeGroupingKey = gks,
              computeProjection = ps
            ).createPartial(inputs = inputsList)
          }
            .fold(err => sys.error(s"Could not generate partial => ${err}"), identity)

          assert(
            pf.outputs.toSet.size == pf.outputs.size,
            "Expected to have distinct outputs from a PF"
          )

          val ff = {
            stagedGroupBy.aggregations
              .map(sa => computeAggregate(sa).map(a => sa -> a))
              .sequence
              .map(ca => GroupByPartialToFinalGenerator(stagedGroupBy, ca).createFinal)
          }
            .fold(err => sys.error(s"Could not generate final => ${err}"), identity)
          val fullFunction = {
            for {
              gks <-
                stagedGroupBy.groupingKeys
                  .map(gk => computeGroupingKey(gk).map(r => gk -> r))
                  .sequence
              cps <- stagedGroupBy.projections
                .map(sp => computeProjection(sp).map(r => sp -> r))
                .sequence
              ca <- stagedGroupBy.aggregations
                .map(sa => computeAggregate(sa).map(a => sa -> a))
                .sequence
            } yield GroupByPartialGenerator(
              GroupByPartialToFinalGenerator(stagedGroupBy, ca),
              computeGroupingKey = gks,
              computeProjection = cps
            )
              .createFull(inputs = inputsList)
          }
            .fold(err => sys.error(s"Could not generate partial => ${err}"), identity)

          val evaluationPlan = NativeAggregationEvaluationPlan(
            outputExpressions = aggregateExpressions,
            functionPrefix = fName,
            evaluationMode =
              if (options.preShufflePartitions.isDefined)
                EvaluationMode.PrePartitioned(fullFunction)
              else EvaluationMode.TwoStaged(pf, ff),
            child = options.preShufflePartitions
              .map { n =>
                ShuffleExchangeExec(
                  outputPartitioning =
                    HashPartitioning(expressions = groupingExpressions, numPartitions = n),
                  child = planLater(child),
                  shuffleOrigin = REPARTITION
                )
              }
              .getOrElse {
                planLater(child)
              },
            nativeEvaluator = nativeEvaluator
          )

          logger.info(s"Plan is: ${evaluationPlan}")

          List(evaluationPlan)

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

  def mapGroupingExpression(expr: Expression, child: LogicalPlan)(implicit
    evalFallback: EvalFallback
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
            SparkVeMapper
              .replaceReferences(prefix = "input_", inputs = child.output.toList, expression = expr)
              .collectFirst {
                case ar: AttributeReference if ar.name.contains("input_") =>
                  Left(StringReference(ar.name.replaceAllLiterally("->data[i]", "")))
              }
          )
          .toRight(s"Cannot support group by: ${expr} (type: ${expr.dataType})")
      case other =>
        SparkVeMapper
          .eval(
            SparkVeMapper
              .replaceReferences(prefix = "input_", inputs = child.output.toList, expression = expr)
          )
          .reportToString
          .map(ce =>
            Right(
              TypedCExpression2(
                veType = SparkVeMapper.sparkTypeToScalarVeType(expr.dataType),
                cExpression = ce
              )
            )
          )
    }
  }
}
