package com.nec.spark

import com.nec.spark.agile.{AveragingPlanner, AveragingSparkPlan}
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan =>
            AveragingPlanner
              .matchPlan(sparkPlan)
              .map { childPlan =>
                AveragingSparkPlan(childPlan, AveragingSparkPlan.averageLocalScala)
              }
              .getOrElse(sparkPlan)
      }
    })
  }
}
