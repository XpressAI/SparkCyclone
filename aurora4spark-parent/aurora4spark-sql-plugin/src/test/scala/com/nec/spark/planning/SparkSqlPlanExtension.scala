package com.nec.spark.planning

import com.nec.spark.planning.SparkSqlPlanExtension.rulesToApply
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

object SparkSqlPlanExtension {
  val rulesToApply = mutable.Buffer.empty[Rule[SparkPlan]]
}
final class SparkSqlPlanExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan =>
            if (rulesToApply.isEmpty) {
              sparkPlan
            } else {
              val resultingPlan = rulesToApply.foldLeft(sparkPlan) { case (plan, rule) =>
                rule.apply(plan)
              }
              assert(
                resultingPlan != sparkPlan,
                sys.error(s"Expected some plan transformations at ${sparkPlan}")
              )
              resultingPlan
            }
      }
    })
  }
}
