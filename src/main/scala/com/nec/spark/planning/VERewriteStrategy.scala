package com.nec.spark.planning

import com.nec.arrow.functions.GroupBySum
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.agile.CExpressionEvaluation.NameCleaner
import com.nec.spark.agile.CExpressionEvaluation.RichListStr
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod
import com.nec.spark.planning.VERewriteStrategy.meldAggregateAndProject
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
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

final case class VERewriteStrategy(sparkSession: SparkSession, nativeEvaluator: NativeEvaluator)
  extends Strategy
  with LazyLogging {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def fName: String = s"eval_${Math.abs(plan.hashCode())}"

    if (VERewriteStrategy._enabled) {
      log.debug(
        s"Processing input plan with VERewriteStrategy: $plan, output types were: ${plan.output.map(_.dataType)}"
      )
      plan match {
        case proj @ logical.Project(
              Seq(
                Alias(
                  Substring(
                    inputExpr,
                    Literal(beginIndex: Int, IntegerType),
                    Literal(endIndex: Int, IntegerType)
                  ),
                  tgt
                )
              ),
              child
            ) =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              fName,
              child.output,
              NewCExpressionEvaluation.evaluate(fName, child.output, tgt, beginIndex, endIndex),
              planLater(child),
              proj.references.map(_.name).toSet,
              nativeEvaluator
            )
          )
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

        case proj @ logical.Project(resultExpressions, logical.Filter(condition, child)) =>
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

        case agg @ logical.Aggregate(
              groupingExpressions,
              resultExpressions,
              prj @ logical.Project(projectList, frs @ logical.Filter(condition, child))
            ) =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              fName,
              resultExpressions,
              List(
                CExpressionEvaluation
                  .cGen(
                    fName = fName,
                    inputReferences =
                      (agg.references ++ frs.references ++ child.references ++ prj.references)
                        .map(_.name)
                        .toSet,
                    childOutputs = child.output,
                    pairs = resultExpressions.map { re =>
                      (
                        re.asInstanceOf[Alias],
                        re
                          .asInstanceOf[Alias]
                          .child
                          .asInstanceOf[AggregateExpression]
                      )
                    },
                    condition = Some(condition)
                  )
                  .lines,
                List("}")
              ).flatten.codeLines,
              planLater(child),
              agg.references.map(_.name).toSet,
              nativeEvaluator
            )
          )
        case logical.Aggregate(groupingExpressions, outerResultExpressions, child)
            if GroupBySum.isLogicalGroupBySum(plan) =>
          List(SimpleGroupBySumPlan(planLater(child), nativeEvaluator, GroupByMethod.VEBased))
        case agg @ logical.Aggregate(
              groupingExpressions,
              outerResultExpressions,
              logical.Project(exprs, child)
            )
            if outerResultExpressions.forall(e =>
              e.isInstanceOf[Alias] && e.asInstanceOf[Alias].child.isInstanceOf[AggregateExpression]
            ) =>
          val resultExpressions =
            try meldAggregateAndProject(outerResultExpressions.toList, exprs.toList)
            catch {
              case e: Throwable =>
                throw new IllegalArgumentException(
                  s"Could not process project+aggregate of $exprs ==> $outerResultExpressions: $e",
                  e
                )
            }
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              fName,
              resultExpressions,
              List(
                CExpressionEvaluation
                  .cGen(
                    fName,
                    agg.references.map(_.name).toSet,
                    child.output,
                    resultExpressions.map { re =>
                      (
                        re.asInstanceOf[Alias],
                        re
                          .asInstanceOf[Alias]
                          .child
                          .asInstanceOf[AggregateExpression]
                      )
                    }
                  )
                  .lines,
                List("}")
              ).flatten.codeLines,
              planLater(child),
              agg.references.map(_.name).toSet,
              nativeEvaluator
            )
          )

        /** There can be plans where we have Cast(Alias(AggEx) as String) - this is not yet supported */
        case agg @ logical.Aggregate(groupingExpressions, resultExpressions, child)
            if resultExpressions.forall(e =>
              e.isInstanceOf[Alias] && e.asInstanceOf[Alias].child.isInstanceOf[AggregateExpression]
            ) =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              fName,
              resultExpressions,
              List(
                CExpressionEvaluation
                  .cGen(
                    fName,
                    agg.references.map(_.name).toSet,
                    child.output,
                    resultExpressions.map { re =>
                      (
                        re.asInstanceOf[Alias],
                        re
                          .asInstanceOf[Alias]
                          .child
                          .asInstanceOf[AggregateExpression]
                      )
                    }
                  )
                  .lines,
                List("}")
              ).flatten.codeLines,
              planLater(child),
              agg.references.map(_.name).toSet,
              nativeEvaluator
            )
          )
        case _ => Nil
      }
    } else Nil
  }
}
