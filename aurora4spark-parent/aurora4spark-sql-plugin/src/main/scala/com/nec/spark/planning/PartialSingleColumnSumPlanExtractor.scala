package com.nec.spark.planning

import com.nec.spark.agile.{Column, PartialSingleColumnSparkPlan}

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Sum}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object PartialSingleColumnSumPlanExtractor {

  def matchPlan(sparkPlan: SparkPlan): Option[PartialSingleColumnSparkPlan] = {
    PartialFunction.condOpt(sparkPlan) {
      case first @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(avg @ Sum(exr), mode, isDistinct, filter, resultId)),
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            parent @ org.apache.spark.sql.execution.exchange
              .ShuffleExchangeExec(
                outputPartitioning,
                third @ org.apache.spark.sql.execution.aggregate
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

        PartialSingleColumnSparkPlan(Seq(first, parent, third, fourth), parent, fourth, Column(indices(colName), colName))
      }
    }
  }
}
