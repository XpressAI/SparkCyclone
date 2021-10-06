package com.nec.spark.planning

import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkVeMapper.EvaluationAttempt._
import com.nec.spark.agile.SparkVeMapper.{sparkTypeToVeType, EvalFallback}
import com.nec.spark.agile.StagedGroupBy.{
  GroupingKey,
  StagedAggregation,
  StagedAggregationAttribute,
  StagedProjection,
  StringReference
}
import com.nec.spark.agile.{DeclarativeAggregationConverter, SparkVeMapper, StagedGroupBy}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  DeclarativeAggregate,
  HyperLogLogPlusPlus
}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StringType

import scala.collection.immutable
import scala.util.Try

object VERewriteStrategy {
  var _enabled: Boolean = true
  var failFast: Boolean = false
}

final case class VERewriteStrategy(nativeEvaluator: NativeEvaluator)
  extends Strategy
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
              (GroupingKey(s"group_${i}", SparkVeMapper.sparkTypeToVeType(e.dataType)), e)
            }.toList

          val projections: List[(StagedProjection, Expression)] =
            aggregateExpressions.zipWithIndex
              .collect {
                case (ar: AttributeReference, idx)
                    if child.output.toList.exists(_.exprId == ar.exprId) =>
                  Option(StagedProjection(s"sp_${idx}", sparkTypeToVeType(ar.dataType)) -> ar)
                case (Alias(agg, name), idx)
                    if agg.find(_.isInstanceOf[DeclarativeAggregate]).nonEmpty =>
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
                case (other @ Alias(_, _), idx) if other.collectFirst {
                      case _: DeclarativeAggregate =>
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

          val stagedGroupBy = StagedGroupBy(
            groupingKeys = groupingExpressionsKeys.map { case (gk, e) => gk },
            finalOutputs = aggregateExpressions.map { namedExp =>
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
                .getOrElse(sys.error(s"Unmatched output: ${namedExp}"))
            }.toList
          )
          val computeGroupingKey: GroupingKey => Option[Either[StringReference, CExpression]] =
            gk =>
              groupingExpressionsKeys.toMap
                .get(gk)
                .map(exp => mapGroupingExpression(exp, child))

          val computeProjection: StagedProjection => Option[Either[StringReference, CExpression]] =
            sp =>
              projections.toMap.get(sp).map {
                case ar: AttributeReference if ar.dataType == StringType =>
                  Left(
                    StringReference(
                      ar.transform(
                        SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                      ).asInstanceOf[AttributeReference]
                        .name
                    )
                  )
                case Alias(ar: AttributeReference, _) if ar.dataType == StringType =>
                  Left(
                    StringReference(
                      ar.transform(
                        SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                      ).asInstanceOf[AttributeReference]
                        .name
                    )
                  )
                case Alias(other, name) =>
                  Right(
                    SparkVeMapper
                      .eval(
                        other.transform(
                          SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                        )
                      )
                      .getOrReport()
                  )
                case other =>
                  Right(
                    SparkVeMapper
                      .eval(
                        other.transform(
                          SparkVeMapper.referenceReplacer(prefix = "input_", child.output.toList)
                        )
                      )
                      .getOrReport()
                  )
              }

          val computeAggregate: StagedAggregation => Option[Aggregation] = sa =>
            aggregates.toMap.get(sa).map {
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

          logDebug(s"Staged groupBy = ${stagedGroupBy}")

          val inputsList = child.output.zipWithIndex.map { case (att, id) =>
            sparkTypeToVeType(att.dataType).makeCVector(s"input_${id}")
          }.toList

          val pf = stagedGroupBy.createPartial(
            inputs = inputsList,
            computeGroupingKey = computeGroupingKey,
            computeProjection = computeProjection,
            computeAggregate = computeAggregate
          )

          assert(
            pf.outputs.toSet.size == pf.outputs.size,
            "Expected to have distinct outputs from a PF"
          )

          val ff = stagedGroupBy.createFinal(computeAggregate)

          List(
            NativeAggregationEvaluationPlan(
              outputExpressions = aggregateExpressions,
              functionPrefix = fName,
              partialFunction = pf,
              finalFunction = ff,
              child = planLater(child),
              nativeEvaluator = nativeEvaluator
            )
          )

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
  ): Either[StringReference, CExpression] = {
    expr.dataType match {
      case StringType =>
        /**
         * This is not correct for any group-by that are not a simple reference.
         * todo fix it
         */
        Left(
          expr
            .find(_.isInstanceOf[AttributeReference])
            .flatMap(expr =>
              SparkVeMapper
                .replaceReferences(
                  prefix = "input_",
                  inputs = child.output.toList,
                  expression = expr
                )
                .collectFirst {
                  case ar: AttributeReference if ar.name.contains("input_") =>
                    StringReference(ar.name.replaceAllLiterally("->data[i]", ""))
                }
            )
            .getOrElse(sys.error(s"Cannot support group by: ${expr} (type: ${expr.dataType})"))
        )
      case other =>
        Right(
          SparkVeMapper
            .eval(
              SparkVeMapper
                .replaceReferences(
                  prefix = "input_",
                  inputs = child.output.toList,
                  expression = expr
                )
            )
            .getOrReport()
        )
    }
  }
}
