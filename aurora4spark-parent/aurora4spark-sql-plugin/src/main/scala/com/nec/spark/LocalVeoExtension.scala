package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.agile._
import com.nec.spark.planning.{AddPlanExtractor, ArrowGenericAggregationPlanOffHeap, VeoGenericPlanExtractor, WordCountPlanner}
import com.nec.spark.planning.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager
import com.nec.spark.planning.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.WordCountPlanner.WordCounter

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] =
          sparkPlan =>
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
              .orElse {
                AddPlanExtractor.matchAddPairwisePlan(
                  sparkPlan,
                  PairwiseAdditionOffHeap.OffHeapPairwiseSummer.VeoBased
                )
              }
              .orElse {
                WordCountPlanner.applyMaybe(sparkPlan, WordCounter.VEBased)
              }
              .getOrElse(sparkPlan)
      }
    })
  }

  def createAggregator(aggregationFunction: AggregationFunction): Aggregator = {
    aggregationFunction match {
      case SumAggregation => new SumAggregator(
        new VeArrowNativeInterfaceNumeric(Aurora4SparkExecutorPlugin.lib)
      )
      case AvgAggregation => new AvgAggregator(
        new VeArrowNativeInterfaceNumeric(Aurora4SparkExecutorPlugin.lib)
      )
    }
  }

  def createExpressionAggregator(aggregationFunction: AggregationExpression): ColumnAggregator = {
    aggregationFunction match {
      case SumExpression      => AdditionAggregator(MultipleColumnsOffHeapSummer.VeoBased)
      case SubtractExpression => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _                  => NoAggregationAggregator
    }
  }
}
