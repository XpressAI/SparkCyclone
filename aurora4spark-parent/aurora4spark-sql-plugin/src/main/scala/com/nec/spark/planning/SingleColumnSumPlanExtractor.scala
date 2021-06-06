package com.nec.spark.planning
import com.nec.spark.agile.{Column, SingleColumnSparkPlan}

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object SingleColumnSumPlanExtractor {

  def matchPlan(sparkPlan: SparkPlan): Option[SingleColumnSparkPlan] = {
    PartialFunction.condOpt(sparkPlan) {
      case first @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(avg @ Sum(exr), mode, isDistinct, filter, resultId)),
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
          ) if (avg.references.size == 1) => {
        val indices = fourth.output.map(_.name).zipWithIndex.toMap
        val colName = avg.references.head.name

        SingleColumnSparkPlan(fourth, Column(indices(colName), colName))
      }
    }
  }
}
