package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

/**
 * Basic SparkPlan matcher that will match a plan that averages a bunch of numbers.
 */
object AveragingPlanner {
  def matchPlan(sparkPlan: SparkPlan): Option[SparkPlan] = {
    PartialFunction
      .condOpt(sparkPlan) {
        case first @ HashAggregateExec(
              requiredChildDistributionExpressions,
              groupingExpressions,
              List(AggregateExpression(Average(_), _, _, _, _)),
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
            ) =>
          fourth
      }
  }

}
