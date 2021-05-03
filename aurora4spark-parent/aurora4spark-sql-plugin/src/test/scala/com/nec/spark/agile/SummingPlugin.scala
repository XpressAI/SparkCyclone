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
object SummingPlugin {
  var enable: Boolean = false
  var summer: BigDecimalSummer = BigDecimalSummer.ScalaSummer
}

final class SummingPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan =>
            if (SummingPlugin.enable) {
              SumPlanExtractor
                .matchSumChildPlan(sparkPlan)
                .map(planWithMetadata =>
                  SummingSparkPlanMultipleColumns(
                    planWithMetadata.sparkPlan,
                    planWithMetadata.attributes,
                    SummingPlugin.summer
                  )
                )
                .getOrElse(sys.error(s"Could not match the plan: ${sparkPlan}"))
            } else sparkPlan
      }
    })
  }
}
