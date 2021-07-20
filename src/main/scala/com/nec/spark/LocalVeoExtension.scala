package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.agile._
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.VeoBased
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.WordCountPlanner.WordCounter
import com.nec.spark.planning.AddPlanExtractor
import com.nec.spark.planning.ArrowAveragingPlan
import com.nec.spark.planning.ArrowGenericAggregationPlanOffHeap
import com.nec.spark.planning.ArrowSummingPlan
import com.nec.spark.planning.AveragingPlanOffHeap
import com.nec.spark.planning.SingleColumnAvgPlanExtractor
import com.nec.spark.planning.SingleColumnSumPlanExtractor
import com.nec.spark.planning.SummingPlanOffHeap
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.planning.VeoGenericPlanExtractor
import com.nec.spark.planning.WordCountPlanner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SparkSessionExtensions

object LocalVeoExtension {
  var _enabled = true
  var _arrowEnabled = true
  var _useCodegenPlans = false

  def createAggregator(aggregationFunction: AggregateFunction): Aggregator = {
    aggregationFunction match {
      case Sum(_) =>
        new SumAggregator(Aurora4SparkExecutorPlugin.veArrowNativeInterfaceNumeric)
      case Average(_) =>
        new AvgAggregator(Aurora4SparkExecutorPlugin.veArrowNativeInterfaceNumeric)
    }
  }

  def createExpressionAggregator(aggregationFunction: Expression): ColumnAggregator = {
    aggregationFunction match {
      case Add(_, _, _) =>
        AdditionAggregator(
          new VeArrowNativeInterfaceNumeric(
            Aurora4SparkExecutorPlugin._veo_proc,
            Aurora4SparkExecutorPlugin.lib
          )
        )
      case Subtract(_, _, _) => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _                 => NoAggregationAggregator
    }
  }

  def preColumnarRule: Rule[SparkPlan] = { sparkPlan =>
    SingleColumnAvgPlanExtractor
      .matchPlan(sparkPlan)
      .map(singleColumnPlan =>
        if (_arrowEnabled) {
          ArrowAveragingPlan(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
        } else {
          AveragingPlanOffHeap(
            singleColumnPlan.sparkPlan,
            MultipleColumnsOffHeapSummer.VeoBased,
            singleColumnPlan.column
          )
        }
      )
      .orElse(
        SingleColumnSumPlanExtractor
          .matchPlan(sparkPlan)
          .map(singleColumnPlan =>
            if (_arrowEnabled) {
              ArrowSummingPlan(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
            } else {
              SummingPlanOffHeap(
                singleColumnPlan.sparkPlan,
                MultipleColumnsOffHeapSummer.VeoBased,
                singleColumnPlan.column
              )
            }
          )
      )
      .orElse {
        VeoGenericPlanExtractor
          .matchPlan(sparkPlan)
          .map { case GenericSparkPlanDescription(sparkPlan, outColumns) =>
            val outputColumns = outColumns.map { case desc =>
              OutputColumn(
                desc.inputColumns,
                desc.outputColumnIndex,
                createExpressionAggregator(desc.columnAggregation),
                createAggregator(desc.outputAggregator)
              )
            }

            if (sparkPlan.supportsColumnar) sparkPlan
            ArrowGenericAggregationPlanOffHeap(
              if (sparkPlan.supportsColumnar) sparkPlan
              else RowToColumnarExec(sparkPlan),
              outputColumns
            )
          }
      }
      .orElse {
        AddPlanExtractor.matchAddPairwisePlan(
          sparkPlan,
          Aurora4SparkExecutorPlugin.veArrowNativeInterfaceNumeric
        )
      }
      .orElse {
        WordCountPlanner.applyMaybe(sparkPlan, WordCounter.VEBased)
      }
      .getOrElse(sparkPlan)
  }

}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      new VERewriteStrategy(sparkSession, ExecutorPluginManagedEvaluator)
    )
  }
}
