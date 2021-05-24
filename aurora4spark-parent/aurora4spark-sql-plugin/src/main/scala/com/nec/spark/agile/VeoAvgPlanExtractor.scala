package com.nec.spark.agile

import com.nec.spark.agile.MultipleColumnsSummingPlanOffHeap.MultipleColumnsOffHeapSummer

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Subtract}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}

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
        val columnMappings = extractExpressions(exprs).zipWithIndex
          .map{
            case ((operation, attributes), id) => ColumnAggregation(
              attributes.map(attr => Column(columnIndices(attr.value), attr.value)),
              operation,
              id
            )
          }

        VeoSparkPlanWithMetadata(fourth, columnMappings)
      }
    }
  }

  def extractExpressions(expressions: Seq[AggregateExpression]):
  Seq[(ColumnAggregator, Seq[AttributeName])] = {
    val attributeNames = expressions.map {
      case AggregateExpression(sum @ Average(expr), _, _, _, _) =>
        sum.toAggregateExpression()
        val references = sum.references
          .map(reference => AttributeName(reference.name))
          .toSeq // Poor thing this is done on Strings can we do better here?
        (extractOperation(expr), references)
    }

    attributeNames
  }

  def extractOperation(expression: Expression): ColumnAggregator = {
    expression match {
      case Add(_, _, _) => AdditionAggregator(MultipleColumnsOffHeapSummer.VeoBased)
      case Subtract(_, _, _) => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _ => NoAggregationAggregator
    }
  }
}
