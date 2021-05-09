package com.nec.spark

import com.nec.VeDirectApp.compile_c
import com.nec.spark.LocalVeoExtension.ve_so_name
import com.nec.spark.agile.AveragingSparkPlanOffHeap.OffHeapDoubleAverager
import com.nec.spark.agile._
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, RowToColumnarExec, SparkPlan}

object LocalVeoExtension {
  lazy val ve_so_name = compile_c()
}
final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan =>
            AveragingPlanner
              .matchPlan(sparkPlan)
              .map { childPlan =>
                AveragingSparkPlanOffHeap(
                  RowToColumnarExec(childPlan.sparkPlan),
                  OffHeapDoubleAverager.VeoBased(ve_so_name)
                )
              }
              .orElse {
                SumPlanExtractor
                  .matchSumChildPlan(sparkPlan)
                  .map { childPlan =>
                    SummingPlanOffHeap(
                      RowToColumnarExec(childPlan.sparkPlan),
                      SummingPlanOffHeap.OffHeapSummer.VeoBased(ve_so_name)
                    )
                  }
              }
              .orElse {
                AddPlanExtractor.matchAddPairwisePlan(
                  sparkPlan,
                  PairwiseAdditionOffHeap.OffHeapPairwiseSummer.VeoBased(ve_so_name)
                )
              }
              .getOrElse(sparkPlan)
      }
    })
  }
}
