package com.nec.spark.planning
import com.nec.spark.agile.AttributeName
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import com.nec.spark.agile.SparkPlanWithMetadata
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.execution.SparkPlan

/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object SumPlanExtractor {
  def matchPlan(sparkPlan: SparkPlan): Option[List[Double]] = {
    matchSumChildPlan(sparkPlan).collectFirst {
      case SparkPlanWithMetadata(LocalTableScanExec(attributes, rows), _) =>
        attributes.toList.zipWithIndex
          .flatMap { case (AttributeReference(_, dataType: DoubleType, _, _), index) =>
            rows
              .map(_.get(index, dataType).asInstanceOf[Double])
              .toList
          }
    }
  }

  def matchSumChildPlan(sparkPlan: SparkPlan): Option[SparkPlanWithMetadata] = {
    PartialFunction.condOpt(sparkPlan) {
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
            case AggregateExpression(Sum(_), _, _, _, _) => true
            case _                                       => false
          } =>
        SparkPlanWithMetadata(fourth, extractExpressions(exprs))
    }
  }

  def extractExpressions(expressions: Seq[AggregateExpression]): Seq[Seq[AttributeName]] = {
    val attributeNames = expressions.map { case AggregateExpression(sum @ Sum(_), _, _, _, _) =>
      sum.references
        .map(reference => AttributeName(reference.name))
        .toSeq // Poor thing this is done on Strings can we do better here?
    }

    attributeNames
  }
}
