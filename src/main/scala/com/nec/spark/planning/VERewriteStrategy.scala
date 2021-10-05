package com.nec.spark.planning

import com.nec.spark.agile.SparkVeMapper.EvaluationAttempt._
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.SparkVeMapper.EvalFallback
import com.nec.spark.agile.{DeclarativeAggregationConverter, SparkVeMapper}
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryArithmetic, ExprId, Expression, KnownNotNull}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SerializeFromObject}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StringType
import scala.collection.immutable
import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}

object VERewriteStrategy {
  var _enabled: Boolean = true
  var failFast: Boolean = false
}

final case class VERewriteStrategy(nativeEvaluator: NativeEvaluator)
  extends Strategy
  with LazyLogging {

  import com.github.ghik.silencer.silent

  def findNotNullable(plan: LogicalPlan): Seq[ExprId] = {
    def traverseChildren(toTraverse: Seq[LogicalPlan], plans: Seq[LogicalPlan]): Seq[LogicalPlan] = {
      toTraverse match {
        case Nil => plans
        case elem :: tail => traverseChildren(tail ++ elem.children, plans :+ elem)
      }
    }
    traverseChildren(Seq(plan), Seq()).collect {
      case SerializeFromObject(serializer, plan) => serializer.collect {
        case exp @ Alias(StaticInvoke(_, _, "fromString", Seq(Invoke(a @ KnownNotNull(arg), _, _, _, _, _)), _, _), _) =>
          exp.exprId
      }
    }.flatten

  }

  @silent
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def fName: String = s"eval_${Math.abs(plan.toString.hashCode())}"

    val failFast = VERewriteStrategy.failFast

    if (VERewriteStrategy._enabled) {
      log.debug(
        s"Processing input plan with VERewriteStrategy: $plan, output types were: ${plan.output.map(_.dataType)}"
      )

      def res: immutable.Seq[SparkPlan] = plan match {
        /** This one is for testing */
//        case proj @ logical.Project(
//              Seq(
//                Alias(
//                  Concat(
//                    Seq(
//                      Substring(
//                        inputExpr,
//                        Literal(beginIndex: Int, IntegerType),
//                        Literal(endIndex: Int, IntegerType)
//                      ),
//                      _,
//                      _
//                    )
//                  ),
//                  _
//                ),
//                Alias(Length(inputExpr2), _),
//                Alias(
//                  Substring(
//                    inputExpr3,
//                    Literal(beginIndex3: Int, IntegerType),
//                    Subtract(Length(inputExpr4), Literal(2, IntegerType), false)
//                  ),
//                  _
//                )
//              ),
//              child
//            ) =>
//          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
//          List(
//            NewCEvaluationPlan(
//              fName,
//              proj.output,
//              StringCExpressionEvaluation.evaluate(fName, child.output, beginIndex, endIndex),
//              planLater(child),
//              proj.references.map(_.name).toSet,
//              nativeEvaluator
//            )
//          )
//        case join @ logical.Join(
//              left,
//              right,
//              Inner,
//              Some(EqualTo(leftKeyExpr, rightKeyExpr)),
//              hint
//            ) => {
//          val leftExprIds = left.output.map(_.exprId).toSet
//          val rightExprIds = right.output.map(_.exprId).toSet
//          val inputs = join.inputSet.toSeq.zipWithIndex.map { case (attr, idx) =>
//            CScalarVector(s"input_${idx}", SparkVeMapper.sparkTypeToScalarVeType(attr.dataType))
//          }
//          val leftKey = TypedCExpression2(
//            SparkVeMapper.sparkTypeToScalarVeType(leftKeyExpr.dataType),
//            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, leftKeyExpr))
//          )
//          val rightKey = TypedCExpression2(
//            SparkVeMapper.sparkTypeToScalarVeType(rightKeyExpr.dataType),
//            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, rightKeyExpr))
//          )
//          val outputs = join.output.zipWithIndex
//            .map((attr) =>
//              NamedJoinExpression(
//                s"output_${attr._2}",
//                SparkVeMapper.sparkTypeToScalarVeType(attr._1.dataType),
//                JoinProjection(
//                  SparkVeMapper.eval(
//                    SparkVeMapper
//                      .replaceReferences(join.inputSet.toSeq, attr._1, leftExprIds, rightExprIds)
//                  )
//                )
//              )
//            )
//            .toList
//
//          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
//
//          List(
//            GeneratedJoinPlan(
//              planLater(left),
//              planLater(right),
//              renderInnerJoin(VeInnerJoin(inputs.toList, leftKey, rightKey, outputs))
//                .toCodeLines(fName),
//              nativeEvaluator,
//              join.inputSet.toSeq,
//              join.output,
//              fName
//            )
//          )
//
//        }

//        case join @ logical.Join(
//              left,
//              right,
//              outerJoin,
//              Some(EqualTo(leftKeyExpr, rightKeyExpr)),
//              hint
//            ) => {
//          val leftExprIds = left.output.map(_.exprId).toSet
//          val rightExprIds = right.output.map(_.exprId).toSet
//          val inputs = join.inputSet.toSeq.zipWithIndex.map { case (attr, idx) =>
//            CScalarVector(s"input_${idx}", SparkVeMapper.sparkTypeToScalarVeType(attr.dataType))
//          }.toList
//          val leftKey = TypedCExpression2(
//            SparkVeMapper.sparkTypeToScalarVeType(leftKeyExpr.dataType),
//            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, leftKeyExpr))
//          )
//          val rightKey = TypedCExpression2(
//            SparkVeMapper.sparkTypeToScalarVeType(rightKeyExpr.dataType),
//            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, rightKeyExpr))
//          )
//          val outputsInner = join.output.zipWithIndex
//            .map((attr) =>
//              NamedJoinExpression(
//                s"output_${attr._2}",
//                SparkVeMapper.sparkTypeToScalarVeType(attr._1.dataType),
//                JoinProjection(
//                  SparkVeMapper.eval(
//                    SparkVeMapper
//                      .replaceReferences(join.inputSet.toSeq, attr._1, leftExprIds, rightExprIds)
//                  )
//                )
//              )
//            )
//            .toList
//          val outerJoinType = outerJoin match {
//            case plans.LeftOuter  => LeftOuterJoin
//            case plans.RightOuter => RightOuterJoin
//          }
//
//          val outputsOuter = join.output.zipWithIndex
//            .map((attr) =>
//              NamedJoinExpression(
//                s"output_${attr._2}",
//                SparkVeMapper.sparkTypeToScalarVeType(attr._1.dataType),
//                JoinProjection(
//                  SparkVeMapper.eval(
//                    SparkVeMapper.replaceReferencesOuter(
//                      join.inputSet.toSeq,
//                      attr._1,
//                      leftExprIds,
//                      rightExprIds,
//                      outerJoinType
//                    )
//                  )
//                )
//              )
//            )
//            .toList
//          val outputs = outputsInner
//            .zip(outputsOuter)
//            .map { case (inner, outer) =>
//              OuterJoinOutput(inner, outer)
//            }
//
//          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
//
//          List(
//            GeneratedJoinPlan(
//              planLater(left),
//              planLater(right),
//              renderOuterJoin(VeOuterJoin(inputs, leftKey, rightKey, outputs, outerJoinType))
//                .toCodeLines(fName),
//              nativeEvaluator,
//              join.inputSet.toSeq,
//              join.output,
//              fName
//            )
//          )
//
//        }

        case agg @ logical.Aggregate(groupingExpressions, aggregateExpressions, child)
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
          implicit val fallback = EvalFallback.noOp
          val groupBySummary: VeGroupBy[CVector, Either[StringGrouping, TypedCExpression2], Either[
            NamedStringProducer,
            NamedGroupByExpression
          ]] = VeGroupBy(
            inputs = agg.inputSet.toList.zipWithIndex.map { case (attr, idx) =>
              if (attr.dataType == StringType)
                CVarChar(s"input_${idx}")
              else
                CScalarVector(s"input_${idx}", SparkVeMapper.sparkTypeToScalarVeType(attr.dataType))
            },
            groups = groupingExpressions.toList.map { expr =>
              expr.dataType match {
                case StringType =>
                  /**
                   * This is not correct for any group-by that are not a simple reference.
                   * todo fix it
                   */
                  Left(
                    expr
                      .find(_.isInstanceOf[AttributeReference])
                      .flatMap(expr => {
                        val nullReplacedExpr = SparkVeMapper.replaceNullability(expr, findNotNullable(plan))
                        SparkVeMapper
                          .replaceReferences(inputs = child.output.toList, expression = nullReplacedExpr)
                          .collectFirst {
                            case ar: AttributeReference if ar.name.contains("input_") =>
                              StringGrouping(ar.name.replaceAllLiterally("->data[i]", ""))
                          }
                      }
                      )
                      .getOrElse(
                        sys.error(s"Cannot support group by: ${expr} (type: ${expr.dataType})")
                      )
                  )
                case other =>
                  Right(
                    TypedCExpression2(
                      SparkVeMapper.sparkTypeToScalarVeType(other),
                      SparkVeMapper
                        .eval(
                          SparkVeMapper
                            .replaceReferences(inputs = child.output.toList, expression = SparkVeMapper.replaceNullability(expr, findNotNullable(plan)))
                        )
                        .getOrReport()
                    )
                  )
              }
            },
            outputs = aggregateExpressions.toList.zipWithIndex.map {
              case (Alias(namedExpression, _), idx) if namedExpression.dataType == StringType =>
                Left(
                  NamedStringProducer(
                    name = s"output_${idx}",
                    stringProducer = SparkVeMapper
                      .evalString(
                        SparkVeMapper.replaceNullability(namedExpression, findNotNullable(plan))
                          .transform(SparkVeMapper.referenceReplacer(child.output.toList))
                      )
                      .getOrReport()
                  )
                )
              case (stringExp, idx) if stringExp.dataType == StringType =>
                Left(
                  NamedStringProducer(
                    name = s"output_${idx}",
                    stringProducer = SparkVeMapper
                      .evalString(
                        SparkVeMapper.replaceNullability(stringExp, findNotNullable(plan))
                          .transform(SparkVeMapper.referenceReplacer(child.output.toList))
                      )
                      .getOrReport()
                  )
                )
              case (namedExpression, idx) =>
                Right(
                  NamedGroupByExpression(
                    name = s"output_${idx}",
                    veType = SparkVeMapper.sparkTypeToScalarVeType(namedExpression.dataType),
                    groupByExpression = namedExpression match {
                      case Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _) =>
                        GroupByExpression.GroupByAggregation(
                          DeclarativeAggregationConverter(
                            SparkVeMapper.replaceNullability(d, findNotNullable(plan)).transform(SparkVeMapper.referenceReplacer(child.output.toList))
                              .asInstanceOf[DeclarativeAggregate]
                          )
                        )
                      case Alias(other, _) if other.collectFirst { case _: DeclarativeAggregate =>
                            ()
                          }.nonEmpty =>
                        GroupByExpression.GroupByAggregation(
                          DeclarativeAggregationConverter
                            .transformingFetch(
                              SparkVeMapper.replaceNullability(other, findNotNullable(plan))
                                .transform(SparkVeMapper.referenceReplacer(child.output.toList))
                            )
                            .getOrElse(
                              sys.error(
                                s"Cannot figure out how to replace: ${other} (${other.getClass})"
                              )
                            )
                        )
                      case other if other.collectFirst { case _: DeclarativeAggregate =>
                            ()
                          }.nonEmpty =>
                        GroupByExpression.GroupByAggregation(
                          DeclarativeAggregationConverter
                            .transformingFetch(
                              SparkVeMapper.replaceNullability(other, findNotNullable(plan))
                                .transform(SparkVeMapper.referenceReplacer(child.output.toList))
                            )
                            .getOrElse(
                              sys.error(
                                s"Cannot figure out how to replace: ${other} (${other.getClass})"
                              )
                            )
                        )
                      case Alias(other, _) =>
                        GroupByExpression.GroupByProjection(
                          SparkVeMapper
                            .eval(
                              SparkVeMapper.replaceNullability(other, findNotNullable(plan)).transform(SparkVeMapper.referenceReplacer(child.output.toList))
                            )
                            .getOrReport()
                        )
                      case other =>
                        GroupByExpression.GroupByProjection(
                          SparkVeMapper
                            .eval(
                              SparkVeMapper.replaceNullability(other, findNotNullable(plan)).transform(SparkVeMapper.referenceReplacer(child.output.toList))
                            )
                            .getOrReport()
                        )
                    }
                  )
                )
            }
          )
          logger.debug(s"Group by = ${groupBySummary}")
          val codeLines = renderGroupBy(groupBySummary).toCodeLines(fName)

          List(
            NewCEvaluationPlan(
              fName,
              aggregateExpressions,
              codeLines,
              planLater(child),
              agg.references.map(_.name).toSet,
              nativeEvaluator
            )
          )

//        case agg @ logical.Aggregate(groupingExpressions, aggregateExpressions, Project(_, child))
//            if aggregateExpressions.collect {
//              case al @ Alias(AggregateExpression(Count(_), _, _, _, _), _) => al
//            }.size == aggregateExpressions.size =>
//          Nil

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
