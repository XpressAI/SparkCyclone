package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.LocalVeoExtension._enabled
import com.nec.spark.agile._
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.VeoBased
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.{AddPlanExtractor, ArrowAveragingPlan, ArrowGenericAggregationPlanOffHeap, ArrowSummingPlan, AveragingPlanOffHeap, SingleColumnAvgPlanExtractor, SingleColumnSumPlanExtractor, SummingPlanOffHeap, VeoGenericPlanExtractor, WordCountPlanner}
import com.nec.spark.planning.WordCountPlanner.WordCounter

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Average, Sum}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan

object LocalVeoExtension {
  var _enabled = true
  var _arrowEnabled = true

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
            Aurora4SparkExecutorPlugin._veo_ctx,
            Aurora4SparkExecutorPlugin.lib
          )
        )
      case Subtract(_, _, _) => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _                  => NoAggregationAggregator
    }
  }

  def preColumnarRule: Rule[SparkPlan] = { sparkPlan =>
    SingleColumnAvgPlanExtractor
      .matchPlan(sparkPlan)
      .map(singleColumnPlan =>
        if(_arrowEnabled) {
          ArrowAveragingPlan(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
        } else {
          AveragingPlanOffHeap(singleColumnPlan.sparkPlan, MultipleColumnsOffHeapSummer.VeoBased,
            singleColumnPlan.column)
        }
      )
      .orElse(
        SingleColumnSumPlanExtractor
          .matchPlan(sparkPlan)
          .map(singleColumnPlan =>
            if(_arrowEnabled) {
              ArrowSummingPlan(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
            } else {
              SummingPlanOffHeap(singleColumnPlan.sparkPlan, MultipleColumnsOffHeapSummer.VeoBased,
                singleColumnPlan.column)
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

    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] = sparkPlan =>
          if (_enabled) LocalVeoExtension.preColumnarRule.apply(sparkPlan) else sparkPlan
      }
    })
  }

}
