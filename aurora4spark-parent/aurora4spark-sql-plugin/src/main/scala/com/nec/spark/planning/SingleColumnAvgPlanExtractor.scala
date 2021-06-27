package com.nec.spark.planning
import com.nec.spark.agile.{Column, SingleColumnSparkPlan}

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object SingleColumnAvgPlanExtractor {

  def matchPlan(sparkPlan: SparkPlan): Option[SingleColumnSparkPlan] = {
    PartialFunction.condOpt(sparkPlan) {
      case first @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(avg @ Average(exr), mode, isDistinct, filter, resultId)),
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
          ) if (avg.references.size == 1) => {
        val indices = fourth.output.map(_.name).zipWithIndex.toMap
        val colName = avg.references.head.name

        SingleColumnSparkPlan(fourth, Column(indices(colName), colName))
      }
    }
  }

//  def matchLogicalPlan(logicalPlan: LogicalPlan): Option[SingleColumnSparkPlan] = {
//    PartialFunction.condOpt(logicalPlan) {
//      case Aggregate(groupingExpressions, aggregateExpressions, child)
//        if(aggregateExpressions.size == 1 && aggregateExpressions.head) =>
//
//      if (avg.references.size == 1) => {
//        val indices = fourth.output.map(_.name).zipWithIndex.toMap
//        val colName = avg.references.head.name
//
//        SingleColumnSparkPlan(fourth, Column(indices(colName), colName))
//      }
//    }
//  }
}
