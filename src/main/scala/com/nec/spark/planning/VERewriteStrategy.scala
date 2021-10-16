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
import org.apache.spark.sql.catalyst.expressions.{
  aggregate,
  Alias,
  AttributeReference,
  Expression,
  NamedExpression
}
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
                  name = s"group_${i}",
                  veType = SparkVeMapper.sparkTypeToVeType(e.dataType)
                ),
                e
              )
            }.toList

          val projectionsE: Either[String, List[(StagedProjection, Expression)]] =
            aggregateExpressions.zipWithIndex
              .map {
                case (ar: AttributeReference, idx)
                    if child.output.toList.exists(_.exprId == ar.exprId) =>
                  Right(
                    Option(StagedProjection(s"sp_${idx}", sparkTypeToVeType(ar.dataType)) -> ar)
                  )
                case (anything, idx)
                    if anything.isInstanceOf[aggregate.AggregateExpression] || anything
                      .find(_.isInstanceOf[DeclarativeAggregate])
                      .nonEmpty =>
                  /** Intentionally ignore -- these are not actual projections */
                  Right(Option.empty)
                case (Alias(exp, name), idx) =>
                  Right(
                    Option(StagedProjection(s"sp_${idx}", sparkTypeToVeType(exp.dataType)) -> exp)
                  )
                case other =>
                  Left(s"Unexpected aggregate expression: ${other}, type ${other._1.getClass}")
              }
              .toList
              .sequence
              .map(_.flatten)

          def doProj(e: Expression): Either[String, Either[StringReference, TypedCExpression2]] =
            e match {
              case ar: AttributeReference if ar.dataType == StringType =>
                Right(Left(StringReference(ar.name)))
              case Alias(ar: AttributeReference, _) if ar.dataType == StringType =>
                Right(Left(StringReference(ar.name)))
              case Alias(other, _) =>
                SparkVeMapper
                  .eval(other)
                  .reportToString
                  .map(ce =>
                    Right(
                      TypedCExpression2(SparkVeMapper.sparkTypeToScalarVeType(other.dataType), ce)
                    )
                  )
              case other =>
                SparkVeMapper
                  .eval(other)
                  .reportToString
                  .map(ce =>
                    Right(
                      TypedCExpression2(SparkVeMapper.sparkTypeToScalarVeType(other.dataType), ce)
                    )
                  )
            }

          def computeAggregate(exp: Expression): Either[String, Aggregation] = exp match {
            case Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _) =>
              Right(DeclarativeAggregationConverter(d))
            case other =>
              DeclarativeAggregationConverter
                .transformingFetch(other)
                .toRight(s"Cannot figure out how to replace: ${other} (${other.getClass})")
          }

          val inputsList = child.output.zipWithIndex.map { case (att, id) =>
            sparkTypeToVeType(att.dataType).makeCVector(s"input_${id}")
          }.toList

          val refRep =
            SparkVeMapper.referenceReplacer(prefix = "input_", inputs = child.output.toList)
          val evaluationPlanE = for {
            projections <- projectionsE
            aggregates <- computeAggregates(aggregateExpressions, refRep)
            finalOutputs <- aggregateExpressions
              .map { namedExp_ =>
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
              .toList
              .sequence
            stagedGroupBy = GroupByOutline(
              groupingKeys = groupingExpressionsKeys.map { case (gk, e) => gk },
              finalOutputs = finalOutputs
            )
            _ = logInfo(s"stagedGroupBy = ${stagedGroupBy}")
            gks <-
              groupingExpressionsKeys.map { case (gk, exp) =>
                mapGroupingExpression(exp, refRep)
                  .map(e => gk -> e)
              }.sequence
            ps <- projections.map { case (sp, p) =>
              doProj(p.transform(refRep)).map(r => sp -> r)
            }.sequence
            ca <- aggregates.map { case (sa, exp) =>
              computeAggregate(exp.transform(refRep))
                .map(r => sa -> r)
            }.sequence
            gpg = GroupByPartialGenerator(
              finalGenerator = GroupByPartialToFinalGenerator(
                stagedGroupBy = stagedGroupBy,
                computedAggregates = ca
              ),
              computedGroupingKeys = gks,
              computedProjections = ps
            )
            pf = gpg.createPartial(inputs = inputsList)
            _ <-
              if (pf.outputs.toSet.size == pf.outputs.size) Right(())
              else Left(s"Expected to have distinct outputs from a PF, got: ${pf.outputs}")
            ff = gpg.finalGenerator.createFinal
            fullFunction =
              gpg.createFull(inputs = inputsList)
          } yield {
            options.preShufflePartitions match {
              case Some(n) =>
                NativeAggregationEvaluationPlan(
                  outputExpressions = aggregateExpressions,
                  functionPrefix = functionPrefix,
                  evaluationMode = EvaluationMode.PrePartitioned(fullFunction),
                  child = ShuffleExchangeExec(
                    outputPartitioning =
                      HashPartitioning(expressions = groupingExpressions, numPartitions = n),
                    child = planLater(child),
                    shuffleOrigin = REPARTITION
                  ),
                  nativeEvaluator = nativeEvaluator
                )
              case None =>
                NativeAggregationEvaluationPlan(
                  outputExpressions = aggregateExpressions,
                  functionPrefix = functionPrefix,
                  evaluationMode = EvaluationMode.TwoStaged(pf, ff),
                  child = planLater(child),
                  nativeEvaluator = nativeEvaluator
                )
            }
          }
          val evaluationPlan = evaluationPlanE.fold(sys.error, identity)
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

  private def computeAggregates(
    aggregateExpressions: Seq[NamedExpression],
    referenceReplacer: PartialFunction[Expression, Expression]
  )(implicit evalFallback: EvalFallback): Either[String, List[(StagedAggregation, Expression)]] = {
    aggregateExpressions.zipWithIndex
      .collect {
        case (o @ Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _), idx) =>
          Right {
            (
              GroupByExpression.GroupByAggregation(
                DeclarativeAggregationConverter(
                  d.transform(referenceReplacer).asInstanceOf[DeclarativeAggregate]
                )
              ),
              o,
              idx
            )
          }
        case (Alias(other, _), idx) if other.collectFirst { case _: DeclarativeAggregate =>
              ()
            }.nonEmpty =>
          DeclarativeAggregationConverter
            .transformingFetch(other.transform(referenceReplacer))
            .toRight(s"Cannot figure out how to replace: ${other} (${other.getClass})")
            .map(ag => (GroupByExpression.GroupByAggregation(ag), other, idx))
        case (other, idx) if other.collectFirst { case _: DeclarativeAggregate =>
              ()
            }.nonEmpty =>
          DeclarativeAggregationConverter
            .transformingFetch(other.transform(referenceReplacer))
            .toRight(s"Cannot figure out how to replace: ${other} (${other.getClass})")
            .map(ag => (GroupByExpression.GroupByAggregation(ag), other, idx))
      }
      .toList
      .sequence
      .map(_.map { case (groupByExpression, expr, idx) =>
        StagedAggregation(
          s"agg_${idx}",
          sparkTypeToVeType(expr.dataType),
          groupByExpression.aggregation.partialValues(s"agg_${idx}").map { case (cs, ce) =>
            StagedAggregationAttribute(name = cs.name, veScalarType = cs.veType)
          }
        ) -> expr
      })
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
        SparkVeMapper
          .eval(expr.transform(refRep))
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
