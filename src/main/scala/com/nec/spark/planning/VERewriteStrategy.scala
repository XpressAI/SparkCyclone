package com.nec.spark.planning
import com.nec.arrow.functions.GroupBySum
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.agile.CExpressionEvaluation.NameCleaner
import com.nec.spark.agile.CExpressionEvaluation.RichListStr
import com.nec.spark.planning.SimpleGroupBySumPlan.GroupByMethod
import com.nec.spark.planning.VERewriteStrategy.meldAggregateAndProject
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

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
  extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    def fName: String = s"eval_${Math.abs(plan.hashCode())}"
    if (VERewriteStrategy._enabled) {
      plan match {
        case proj @ logical.Project(resultExpressions, child) if !resultExpressions.forall {
              /** If it's just a rename, don't send to VE * */
              case a: Alias if a.child.isInstanceOf[Attribute] => true
              case _                                           => false
            } =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              fName,
              resultExpressions,
              CExpressionEvaluation
                .cGenProject(fName, proj.references.map(_.name).toSet, child.output, resultExpressions),
              planLater(child),
              proj.references.map(_.name).toSet,
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
                    }: _*
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
                    }: _*
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
