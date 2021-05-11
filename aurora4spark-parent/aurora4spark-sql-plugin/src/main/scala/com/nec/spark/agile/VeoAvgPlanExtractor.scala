package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.types.DoubleType

/**
 * Basic SparkPlan matcher that will match a plan that sums a bunch of BigDecimals, and gets them
 * from the Local Spark table.
 *
 * This is done so that we have something basic to work with.
 */
object VeoAvgPlanExtractor {

  def matchPlan(sparkPlan: SparkPlan): Option[VeoSparkPlanWithMetadata] = {
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
                    fourth @ LocalTableScanExec(output, rows)
                  ),
                shuffleOrigin
              )
          ) if seq.forall {
            case AggregateExpression(Average(_), _, _, _, _) => true
            case _                                       => false
          } => {
        val columnIndices = output.map(_.name).zipWithIndex.toMap
        val columnMappings = extractExpressions(exprs)
          .map(attributes => attributes.map(attribute => columnIndices(attribute.value)))

        VeoSparkPlanWithMetadata(fourth, columnMappings)
      }
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
