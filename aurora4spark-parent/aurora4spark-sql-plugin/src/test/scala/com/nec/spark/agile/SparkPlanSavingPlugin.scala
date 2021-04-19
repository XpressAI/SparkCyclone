package com.nec.spark.agile

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

object SparkPlanSavingPlugin {

  /**
   * This is a hack to capture the Spark plan - will refactor if find a better way to do this.
   *
   * The other way to do this is to use the Spark namespace to access the private values for the
   * plans.
   */
  var savedSparkPlan: SparkPlan = _
}
final class SparkPlanSavingPlugin extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {

    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan => {
            logInfo(s"Pre-columnar Spark plan input: $sparkPlan")
            SparkPlanSavingPlugin.savedSparkPlan = sparkPlan
            sparkPlan
          }

        override def postColumnarTransitions: Rule[SparkPlan] =
          sparkPlan => {
            logInfo(s"Post-columnar Spark plan input: $sparkPlan")
            sparkPlan
          }
      }
    })
  }
}
