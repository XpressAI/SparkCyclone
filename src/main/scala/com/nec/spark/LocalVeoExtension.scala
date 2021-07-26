package com.nec.spark

import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.agile._
import com.nec.spark.planning.VERewriteStrategy

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Average, Sum}

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
      case Add(_, _) =>
        AdditionAggregator(
          new VeArrowNativeInterfaceNumeric(
            Aurora4SparkExecutorPlugin._veo_proc,
            Aurora4SparkExecutorPlugin.lib
          )
        )
      case Subtract(_, _) => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _                 => NoAggregationAggregator
    }
  }

}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      new VERewriteStrategy(sparkSession, ExecutorPluginManagedEvaluator)
    )
  }
}
