package com.nec.spark.planning
import com.nec.arrow.{ArrowNativeInterfaceNumeric, VeArrowNativeInterfaceNumeric}
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.agile.AdditionAggregator
import com.nec.spark.agile.AttributeName

import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import com.nec.spark.agile.Column
import com.nec.spark.agile.ColumnAggregation
import com.nec.spark.agile.ColumnAggregator
import com.nec.spark.agile.MultipleColumnsOffHeapSubtractor
import com.nec.spark.agile.NoAggregationAggregator
import com.nec.spark.agile.SubtractionAggregator
import com.nec.spark.agile.VeoSparkPlanWithMetadata
import com.nec.spark.planning.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer

import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object ArrowVeoSumPlanExtractor {

  def matchPlan(sparkPlan: SparkPlan,
                arrowNativeInterfaceNumeric: ArrowNativeInterfaceNumeric): Option[SparkPlan] = {
    PartialFunction.condOpt(sparkPlan) {
      case first@HashAggregateExec(
      requiredChildDistributionExpressions,
      groupingExpressions,
      Seq(AggregateExpression(avg@Sum(exr), mode, isDistinct, filter, resultId)),
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      org.apache.spark.sql.execution.exchange
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
      ) if(avg.references.size == 1) => {
        val indices = fourth.output.map(_.name).zipWithIndex.toMap
        val colName = avg.references.head.name

        ArrowSummingPlanOffHeap(
          if (fourth.supportsColumnar) fourth else RowToColumnarExec(fourth),
          arrowNativeInterfaceNumeric,
          Column(indices(colName), colName)
        )
      }
    }
  }
}