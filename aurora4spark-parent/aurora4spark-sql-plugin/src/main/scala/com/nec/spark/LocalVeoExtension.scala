package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.LocalVeoExtension.{_enabled, _useCodegenPlans}
import com.nec.spark.agile._
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer
import com.nec.spark.planning.ArrowSummingPlan.ArrowSummer.VeoBased
import com.nec.spark.planning.SummingPlanOffHeap.MultipleColumnsOffHeapSummer
import com.nec.spark.planning.WordCountPlanner.WordCounter
import com.nec.spark.planning.{AddPlanExtractor, ArrowAveragingPlan, ArrowGenericAggregationPlanOffHeap, ArrowSummingCodegenPlan, ArrowSummingPlan, AveragingPlanOffHeap, SingleColumnAvgPlanExtractor, SingleColumnSumPlanExtractor, SummingPlanOffHeap, VeoGenericPlanExtractor, WordCountPlanner}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Average, Sum}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Subtract}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.{SparkSessionExtensions, Strategy}

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
//  def plannerStrategy(logicalPlan: LogicalPlan): Seq[SparkPlan] ={
//    logicalPlan match {
//
//    }
//  } { sparkPlan =>
//    SingleColumnAvgPlanExtractor
//      .matchPlan(sparkPlan)
//      .map(singleColumnPlan =>
//        if(_arrowEnabled) {
//          ArrowAveragingPlan(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
//        } else {
//          AveragingPlanOffHeap(singleColumnPlan.sparkPlan, MultipleColumnsOffHeapSummer.VeoBased,
//            singleColumnPlan.column)
//        }
//      )
//      .orElse(
//        SingleColumnSumPlanExtractor
//          .matchPlan(sparkPlan)
//          .map(singleColumnPlan =>
//            if(_arrowEnabled) {
//              ArrowSummingPlan(singleColumnPlan.sparkPlan, VeoBased, singleColumnPlan.column)
//            } else {
//              SummingPlanOffHeap(singleColumnPlan.sparkPlan, MultipleColumnsOffHeapSummer.VeoBased,
//                singleColumnPlan.column)
//            }
//          )
//      )
//      .orElse {
//        VeoGenericPlanExtractor
//          .matchPlan(sparkPlan)
//          .map { case GenericSparkPlanDescription(sparkPlan, outColumns) =>
//            val outputColumns = outColumns.map { case desc =>
//              OutputColumn(
//                desc.inputColumns,
//                desc.outputColumnIndex,
//                createExpressionAggregator(desc.columnAggregation),
//                createAggregator(desc.outputAggregator)
//              )
//            }
//
//            if (sparkPlan.supportsColumnar) sparkPlan
//            ArrowGenericAggregationPlanOffHeap(
//              if (sparkPlan.supportsColumnar) sparkPlan
//              else RowToColumnarExec(sparkPlan),
//              outputColumns
//            )
//          }
//      }
//      .orElse {
//        AddPlanExtractor.matchAddPairwisePlan(
//          sparkPlan,
//          Aurora4SparkExecutorPlugin.veArrowNativeInterfaceNumeric
//        )
//      }
//      .orElse {
//        WordCountPlanner.applyMaybe(sparkPlan, WordCounter.VEBased)
//      }
//      .getOrElse(sparkPlan)
//  }
}
final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    if (!_useCodegenPlans) {
      sparkSessionExtensions.injectColumnar({ sparkSession =>
        new ColumnarRule {
          override def preColumnarTransitions: Rule[SparkPlan] = sparkPlan =>
            if (_enabled) LocalVeoExtension.preColumnarRule.apply(sparkPlan) else sparkPlan
        }
      })
    } else if(_useCodegenPlans && _enabled) {
      sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
        new Strategy {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] =
            plan match {
              case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                List(ArrowSummingCodegenPlan(planLater(child), ArrowSummer.VeoBased))
              case _ => Nil
            }
        }
      )
    }
  }
}
