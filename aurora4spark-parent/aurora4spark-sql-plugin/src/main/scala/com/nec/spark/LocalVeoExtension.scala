package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.LocalVeoExtension._enabled
import com.nec.spark.agile._
import com.nec.spark.planning.ArrowSummingPlanOffHeap.OffHeapSummer.VeoBased
import com.nec.spark.planning.{
  AddPlanExtractor,
  ArrowAveragingPlanOffHeap,
  ArrowGenericAggregationPlanOffHeap,
  ArrowSummingPlanOffHeap,
  ArrowVeoAvgPlanExtractor,
  ArrowVeoSumPlanExtractor,
  VeoGenericPlanExtractor,
  WordCountPlanner
}
import com.nec.spark.planning.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.WordCountPlanner.WordCounter

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan

object LocalVeoExtension {
  var _enabled = true

  def createAggregator(aggregationFunction: AggregationFunction): Aggregator = {
    aggregationFunction match {
      case SumAggregation =>
        new SumAggregator(Aurora4SparkExecutorPlugin.veArrowNativeInterfaceNumeric)
      case AvgAggregation =>
        new AvgAggregator(Aurora4SparkExecutorPlugin.veArrowNativeInterfaceNumeric)
    }
  }

  def createExpressionAggregator(aggregationFunction: AggregationExpression): ColumnAggregator = {
    aggregationFunction match {
      case SumExpression =>
        AdditionAggregator(
          new VeArrowNativeInterfaceNumeric(
            Aurora4SparkExecutorPlugin._veo_proc,
            Aurora4SparkExecutorPlugin._veo_ctx,
            Aurora4SparkExecutorPlugin.lib
          )
        )
      case SubtractExpression => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _                  => NoAggregationAggregator
    }
  }

  def preColumnarRule: Rule[SparkPlan] = { sparkPlan =>
    ArrowVeoAvgPlanExtractor
      .matchPlan(sparkPlan)
      .map(singleColumnPlan =>
        ArrowAveragingPlanOffHeap(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
      )
      .orElse(
        ArrowVeoSumPlanExtractor
          .matchPlan(sparkPlan)
          .map(singleColumnPlan =>
            ArrowSummingPlanOffHeap(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
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

    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] = sparkPlan =>
          if (_enabled) LocalVeoExtension.preColumnarRule.apply(sparkPlan) else sparkPlan
      }
    })
  }

}
