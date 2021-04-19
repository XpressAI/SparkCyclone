package com.nec.spark.agile

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * This is a test plug-in that rewrites a plan in the form of [[SumPlanExtractor]] into another plan
 * that returns a constant value.
 *
 * This is not how we will do it in production, however it is very important to do this for the
 * proof of concept.
 */
object SumOverNecSshPlugin {
  var enable: Boolean = false
}

final class SumOverNecSshPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan =>
            if (SumOverNecSshPlugin.enable)
              SumPlanExtractor
                .matchPlan(sparkPlan)
                .map(numsToSum =>
                  SingleValueStubPlan
                    .forNumber(BigDecimalSummer.pythonNecSummer.sum(numsToSum))
                )
                .getOrElse(sys.error(s"Could not match the plan: ${sparkPlan}"))
            else sparkPlan
      }
    })
  }
}
