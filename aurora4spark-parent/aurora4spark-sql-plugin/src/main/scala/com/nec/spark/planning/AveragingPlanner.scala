package com.nec.spark.planning
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.SparkPlan
import com.nec.spark.agile.AttributeName
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import com.nec.spark.agile.SparkPlanWithMetadata
import org.apache.spark.sql.catalyst.expressions.aggregate.Average

/**
 * Basic SparkPlan matcher that will match a plan that averages a bunch of numbers.
 */
object AveragingPlanner {
  def matchPlan(sparkPlan: SparkPlan): Option[SparkPlanWithMetadata] = {
    PartialFunction
      .condOpt(sparkPlan) {
        case first @ HashAggregateExec(
              requiredChildDistributionExpressions,
              groupingExpressions,
              exprs @ seq,
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
            ) if seq.forall {
              case AggregateExpression(Average(_), _, _, _, _) => true
              case _                                           => false
            } =>
          SparkPlanWithMetadata(fourth, extractExpressions(exprs))
      }
  }

  def extractExpressions(expressions: Seq[AggregateExpression]): Seq[Seq[AttributeName]] = {
    val attributeNames = expressions.map { case AggregateExpression(sum @ Average(_), _, _, _, _) =>
      sum.references
        .map(reference => AttributeName(reference.name))
        .toSeq // Poor thing this is done on Strings can we do better here?
    }

    attributeNames
  }
}
