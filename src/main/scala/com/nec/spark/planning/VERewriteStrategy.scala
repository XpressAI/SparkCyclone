package com.nec.spark.planning

import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.NameCleaner
import com.nec.spark.agile.CFunctionGeneration.JoinExpression.JoinProjection
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.{CExpressionEvaluation, DeclarativeAggregationConverter, SparkVeMapper}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  Count,
  DeclarativeAggregate
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  Concat,
  EqualTo,
  IsNotNull,
  Length,
  Literal,
  NamedExpression,
  SortOrder,
  Substring,
  Subtract
}
import org.apache.spark.sql.catalyst.plans
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.{logical, Inner}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.IntegerType

object VERewriteStrategy {
  var _enabled: Boolean = true

  def meldAggregateAndProject(
    inputColumnsA: List[NamedExpression],
    inputColumnsB: List[NamedExpression]
  ): List[NamedExpression] = {
    val outputAliases = inputColumnsB.collect { case a: Alias =>
      a
    }
    inputColumnsA.map { expr =>
      expr
        .transformUp {
          case ar @ AttributeReference(name, _, _, _)
              if outputAliases.exists(_.exprId == ar.exprId) =>
            outputAliases.find(_.exprId == ar.exprId).map(_.child).get

          /*          case other if {
                          println(
                            s"Unmatched: ${other}[${other.getClass}]; ${inputColumnsB}; ${inputColumnsB.map(_.getClass)}"
                          ); false
                        } =>
                      ???*/
        }
        .asInstanceOf[NamedExpression]
    }
  }
}

final case class VERewriteStrategy(nativeEvaluator: NativeEvaluator)
  extends Strategy
  with LazyLogging {

  import com.github.ghik.silencer.silent

  @silent
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def fName: String = s"eval_${Math.abs(plan.toString.hashCode())}"

    if (VERewriteStrategy._enabled) {
      log.debug(
        s"Processing input plan with VERewriteStrategy: $plan, output types were: ${plan.output.map(_.dataType)}"
      )

      plan match {
        case proj @ logical.Project(
              Seq(
                Alias(
                  Concat(
                    Seq(
                      Substring(
                        inputExpr,
                        Literal(beginIndex: Int, IntegerType),
                        Literal(endIndex: Int, IntegerType)
                      ),
                      _,
                      _
                    )
                  ),
                  _
                ),
                Alias(Length(inputExpr2), _),
                Alias(
                  Substring(
                    inputExpr3,
                    Literal(beginIndex3: Int, IntegerType),
                    Subtract(Length(inputExpr4), Literal(2, IntegerType), false)
                  ),
                  _
                )
              ),
              child
            ) =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            NewCEvaluationPlan(
              fName,
              proj.output,
              StringCExpressionEvaluation.evaluate(fName, child.output, beginIndex, endIndex),
              planLater(child),
              proj.references.map(_.name).toSet,
              nativeEvaluator
            )
          )
        case join @ logical.Join(
              left,
              right,
              Inner,
              Some(EqualTo(leftKeyExpr, rightKeyExpr)),
              hint
            ) => {
          val leftExprIds = left.output.map(_.exprId).toSet
          val rightExprIds = right.output.map(_.exprId).toSet
          val inputs = join.inputSet.toSeq.zipWithIndex.map { case (attr, idx) =>
            CVector(s"input_${idx}", SparkVeMapper.sparkTypeToVeType(attr.dataType))
          }
          val leftKey = TypedCExpression2(
            SparkVeMapper.sparkTypeToVeType(leftKeyExpr.dataType),
            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, leftKeyExpr))
          )
          val rightKey = TypedCExpression2(
            SparkVeMapper.sparkTypeToVeType(rightKeyExpr.dataType),
            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, rightKeyExpr))
          )
          val outputs = join.output.zipWithIndex
            .map((attr) =>
              NamedJoinExpression(
                s"output_${attr._2}",
                SparkVeMapper.sparkTypeToVeType(attr._1.dataType),
                JoinProjection(
                  SparkVeMapper.eval(
                    SparkVeMapper
                      .replaceReferences(join.inputSet.toSeq, attr._1, leftExprIds, rightExprIds)
                  )
                )
              )
            )
            .toList

          implicit val nameCleaner: NameCleaner = NameCleaner.verbose

          List(
            GeneratedJoinPlan(
              planLater(left),
              planLater(right),
              renderInnerJoin(VeInnerJoin(inputs.toList, leftKey, rightKey, outputs))
                .toCodeLines(fName),
              nativeEvaluator,
              join.inputSet.toSeq,
              join.output,
              fName
            )
          )

        }

        case join @ logical.Join(
              left,
              right,
              outerJoin,
              Some(EqualTo(leftKeyExpr, rightKeyExpr)),
              hint
            ) => {
          val leftExprIds = left.output.map(_.exprId).toSet
          val rightExprIds = right.output.map(_.exprId).toSet
          val inputs = join.inputSet.toSeq.zipWithIndex.map { case (attr, idx) =>
            CVector(s"input_${idx}", SparkVeMapper.sparkTypeToVeType(attr.dataType))
          }.toList
          val leftKey = TypedCExpression2(
            SparkVeMapper.sparkTypeToVeType(leftKeyExpr.dataType),
            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, leftKeyExpr))
          )
          val rightKey = TypedCExpression2(
            SparkVeMapper.sparkTypeToVeType(rightKeyExpr.dataType),
            SparkVeMapper.eval(SparkVeMapper.replaceReferences(join.inputSet.toSeq, rightKeyExpr))
          )
          val outputsInner = join.output.zipWithIndex
            .map((attr) =>
              NamedJoinExpression(
                s"output_${attr._2}",
                SparkVeMapper.sparkTypeToVeType(attr._1.dataType),
                JoinProjection(
                  SparkVeMapper.eval(
                    SparkVeMapper
                      .replaceReferences(join.inputSet.toSeq, attr._1, leftExprIds, rightExprIds)
                  )
                )
              )
            )
            .toList
          val outerJoinType = outerJoin match {
            case plans.LeftOuter  => LeftOuterJoin
            case plans.RightOuter => RightOuterJoin
          }

          val outputsOuter = join.output.zipWithIndex
            .map((attr) =>
              NamedJoinExpression(
                s"output_${attr._2}",
                SparkVeMapper.sparkTypeToVeType(attr._1.dataType),
                JoinProjection(
                  SparkVeMapper.eval(
                    SparkVeMapper.replaceReferencesOuter(
                      join.inputSet.toSeq,
                      attr._1,
                      leftExprIds,
                      rightExprIds,
                      outerJoinType
                    )
                  )
                )
              )
            )
            .toList
          val outputs = outputsInner
            .zip(outputsOuter)
            .map { case (inner, outer) =>
              OuterJoinOutput(inner, outer)
            }

          implicit val nameCleaner: NameCleaner = NameCleaner.verbose

          List(
            GeneratedJoinPlan(
              planLater(left),
              planLater(right),
              renderOuterJoin(VeOuterJoin(inputs, leftKey, rightKey, outputs, outerJoinType))
                .toCodeLines(fName),
              nativeEvaluator,
              join.inputSet.toSeq,
              join.output,
              fName
            )
          )

        }
        case proj @ logical.Project(resultExpressions, child) if !resultExpressions.forall {
              /** If it's just a rename, don't send to VE * */
              case a: Alias if a.child.isInstanceOf[Attribute] => true
              case a: AttributeReference                       => true
              case _                                           => false
            } =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          try {
            List(
              CEvaluationPlan(
                fName,
                resultExpressions,
                CExpressionEvaluation
                  .cGenProject(
                    fName = fName,
                    inputReferences = proj.references.map(_.name).toSet,
                    childOutputs = child.output,
                    resultExpressions = resultExpressions,
                    maybeFilter = None
                  ),
                planLater(child),
                proj.references.map(_.name).toSet,
                nativeEvaluator
              )
            )
          } catch {
            case e: Throwable =>
              throw new RuntimeException(s"Could not match: ${proj} due to $e", e)
          }

        case proj @ logical.Project(resultExpressions, logical.Filter(condition, child))
            if (!condition.isInstanceOf[IsNotNull]) =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          try List(
            CEvaluationPlan(
              fName,
              resultExpressions,
              CExpressionEvaluation
                .cGenProject(
                  fName = fName,
                  inputReferences = proj.references.map(_.name).toSet,
                  childOutputs = child.output,
                  resultExpressions = resultExpressions,
                  maybeFilter = Some(condition)
                ),
              planLater(child),
              proj.references.map(_.name).toSet,
              nativeEvaluator
            )
          )
          catch {
            case e: Throwable =>
              throw new RuntimeException(s"Could not match: ${proj} due to $e", e)
          }
        case sort @ logical.Sort(
              Seq(SortOrder(a @ AttributeReference(_, _, _, _), _, _, _)),
              true,
              child
            ) => {
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            SimpleSortPlan(
              fName,
              sort.inputSet.toSeq,
              CExpressionEvaluation.cGenSort(fName, sort.output, a),
              planLater(child),
              sort.references.map(_.name).toSet,
              nativeEvaluator
            )
          )
        }

        case agg @ logical.Aggregate(groupingExpressions, aggregateExpressions, child)
            if child.output.nonEmpty =>
          val functionName = "dynnen"
          val codeLines = renderGroupBy(
            VeGroupBy(
              inputs = agg.inputSet.toList.zipWithIndex.map { case (attr, idx) =>
                CVector(s"input_${idx}", SparkVeMapper.sparkTypeToVeType(attr.dataType))
              },
              groups = groupingExpressions.toList.map { expr =>
                TypedCExpression2(
                  SparkVeMapper.sparkTypeToVeType(expr.dataType),
                  SparkVeMapper.eval(
                    SparkVeMapper.replaceReferences(inputs = child.output.toList, expression = expr)
                  )
                )
              },
              outputs = aggregateExpressions.toList.zipWithIndex.map {
                case (namedExpression, idx) =>
                  NamedGroupByExpression(
                    name = s"output_${idx}",
                    veType = SparkVeMapper.sparkTypeToVeType(namedExpression.dataType),
                    groupByExpression = namedExpression match {
                      case Alias(AggregateExpression(d: DeclarativeAggregate, _, _, _, _), _) =>
                        GroupByExpression.GroupByAggregation(
                          DeclarativeAggregationConverter(
                            d.transform(SparkVeMapper.referenceReplacer(child.output.toList))
                              .asInstanceOf[DeclarativeAggregate]
                          )
                        )
                      case other =>
                        GroupByExpression.GroupByProjection(
                          SparkVeMapper.eval(
                            other.transform(SparkVeMapper.referenceReplacer(child.output.toList))
                          )
                        )
                    }
                  )
              }
            )
          ).toCodeLines(functionName)

          List(
            NewCEvaluationPlan(
              functionName,
              aggregateExpressions,
              codeLines,
              planLater(child),
              agg.references.map(_.name).toSet,
              nativeEvaluator
            )
          )

        case agg @ logical.Aggregate(groupingExpressions, aggregateExpressions, Project(_, child))
            if aggregateExpressions.collect {
              case al @ Alias(AggregateExpression(Count(_), _, _, _, _), _) => al
            }.size == aggregateExpressions.size =>
          Nil

        case _ => Nil
      }
    } else Nil
  }
}
