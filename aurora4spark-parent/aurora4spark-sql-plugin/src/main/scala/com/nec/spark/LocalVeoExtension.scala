package com.nec.spark

import com.nec.VeDirectApp.compile_c
import com.nec.spark.LocalVeoExtension.ve_so_name
import com.nec.spark.agile.MultipleColumnsAveragingPlanOffHeap.MultipleColumnsOffHeapAverager
import com.nec.spark.agile.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.agile.WordCountPlanner.WordCounter
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
                GenericAggregationPlanOffHeap(
                  if (sparkPlan.supportsColumnar) sparkPlan
                  else RowToColumnarExec(sparkPlan),
                  outputColumns
                )
              }
              .orElse {
                AddPlanExtractor.matchAddPairwisePlan(
                  sparkPlan,
                  PairwiseAdditionOffHeap.OffHeapPairwiseSummer.VeoBased(ve_so_name)
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
      case SumAggregation => new SumAggregator(MultipleColumnsOffHeapSummer.VeoBased)
      case AvgAggregation => new AvgAggregator(MultipleColumnsOffHeapAverager.VeoBased)
    }
  }

  def createExpressionAggregator(aggregationFunction: AggregationExpression): ColumnAggregator = {
    aggregationFunction match {
      case SumExpression => AdditionAggregator(MultipleColumnsOffHeapSummer.VeoBased)
      case SubtractExpression => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _ => NoAggregationAggregator
    }
  }
}
