package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.execution.{LocalTableScanExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import com.nec.spark.agile.{Column, SingleColumnSparkPlan}
/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object ArrowVeoAvgPlanExtractor {

  def matchPlan(sparkPlan: SparkPlan,
                arrowNativeInterfaceNumeric: ArrowNativeInterfaceNumeric): Option[SparkPlan] = {
    PartialFunction.condOpt(sparkPlan) {
      case first @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(avg  @ Average(exr), mode, isDistinct, filter, resultId)),
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
                    fourth @ sparkPlan
                  ),
                shuffleOrigin
              )
          ) if(avg.references.size == 1) => {
          val indices = fourth.output.map(_.name).zipWithIndex.toMap
          val colName = avg.references.head.name

          ArrowAveragingPlanOffHeap(
            if (fourth.supportsColumnar) fourth else RowToColumnarExec(fourth),
            arrowNativeInterfaceNumeric,
            Column(indices(colName), colName)
          )
      }
    }
  }
}
