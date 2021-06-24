package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.LocalVeoExtension._enabled
import com.nec.spark.agile._
import com.nec.spark.agile.wscg.ArrowSummingCodegenPlan
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.VeoBased
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.AddPlanExtractor
import com.nec.spark.planning.ArrowAveragingPlan
import com.nec.spark.planning.ArrowGenericAggregationPlanOffHeap
import com.nec.spark.planning.ArrowSummingPlan
import com.nec.spark.planning.AveragingPlanOffHeap
import com.nec.spark.planning.SingleColumnAvgPlanExtractor
import com.nec.spark.planning.SingleColumnSumPlanExtractor
import com.nec.spark.planning.SummingPlanOffHeap
import com.nec.spark.planning.VeoGenericPlanExtractor
import com.nec.spark.planning.WordCountPlanner
import com.nec.spark.planning.WordCountPlanner.WordCounter
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.RowToColumnarExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

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
      case _                 => NoAggregationAggregator
    }
  }

  def preColumnarRule: Rule[SparkPlan] = { sparkPlan =>
    PartialFunction
      .condOpt(sparkPlan) {
        case first @ HashAggregateExec(
              requiredChildDistributionExpressions,
              groupingExpressions,
              Seq(AggregateExpression(avg @ Sum(exr), mode, isDistinct, filter, resultId)),
              aggregateAttributes,
              initialInputBufferOffset,
              resultExpressions,
              see @ org.apache.spark.sql.execution.exchange
                .ShuffleExchangeExec(
                  outputPartitioning,
                  org.apache.spark.sql.execution.aggregate
                    .HashAggregateExec(
                      _requiredChildDistributionExpressions,
                      _groupingExpressions,
                      _aggregateExpressions,
                      _aggregateAttributes,
                      _initialInputBufferOffset,
                      _resultExpressions,
                      fourth
                    ),
                  shuffleOrigin
                )
            ) if (avg.references.size == 1) => {
          println(fourth)
          println(fourth.getClass.getCanonicalName())
          println(fourth.supportsColumnar)
          val indices = fourth.output.map(_.name).zipWithIndex.toMap
          val colName = avg.references.head.name

          first.copy(child =
            see.copy(child =
              ArrowSummingCodegenPlan(fourth, VeoBased)
            )
          )
        }
      }
      .orElse {
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
      }
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

    sparkSessionExtensions.injectColumnar({ sparkSession =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] = sparkPlan =>
          if (_enabled) LocalVeoExtension.preColumnarRule.apply(sparkPlan) else sparkPlan
      }
    })
  }

}
