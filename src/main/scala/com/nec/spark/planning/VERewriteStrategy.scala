package com.nec.spark.planning
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import com.nec.spark.agile.CExpressionEvaluation.NameCleaner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.agile.CExpressionEvaluation.RichListStr
import com.nec.spark.planning.CEvaluationPlan.NativeEvaluator
import com.nec.spark.planning.VERewriteStrategy.meldAggregateAndProject
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression

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
final class VERewriteStrategy(sparkSession: SparkSession, nativeEvaluator: NativeEvaluator)
  extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (VERewriteStrategy._enabled) {
      plan match {
        case logical.Project(resultExpressions, child) if !resultExpressions.forall {
              /** If it's just a rename, don't send to VE * */
              case a: Alias if a.child.isInstanceOf[Attribute] => true
              case _                                           => false
            } =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              resultExpressions,
              CExpressionEvaluation
                .cGenProject(child.output, resultExpressions),
              planLater(child),
              nativeEvaluator
            )
          )
        case logical.Aggregate(
              groupingExpressions,
              outerResultExpressions,
              logical.Project(exprs, child)
            ) =>
          val resultExpressions =
            meldAggregateAndProject(outerResultExpressions.toList, exprs.toList)
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              resultExpressions,
              List(
                CExpressionEvaluation
                  .cGen(
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
              nativeEvaluator
            )
          )
        case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
          implicit val nameCleaner: NameCleaner = NameCleaner.verbose
          List(
            CEvaluationPlan(
              resultExpressions,
              List(
                CExpressionEvaluation
                  .cGen(
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
              nativeEvaluator
            )
          )
        case _ => Nil
      }
    } else Nil
  }
}
