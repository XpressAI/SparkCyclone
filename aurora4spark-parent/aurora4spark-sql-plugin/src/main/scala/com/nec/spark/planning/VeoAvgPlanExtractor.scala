package com.nec.spark.planning
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.agile.AdditionAggregator
import com.nec.spark.agile.AttributeName
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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression

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
            case _                                           => false
          } => {
        val columnIndices = output.map(_.name).zipWithIndex.toMap
        val columnMappings = extractExpressions(exprs).zipWithIndex
          .map { case ((operation, attributes), id) =>
            ColumnAggregation(
              attributes.map(attr => Column(columnIndices(attr.value), attr.value)),
              operation,
              id
            )
          }

        VeoSparkPlanWithMetadata(fourth, columnMappings)
      }
    }
  }

  def extractExpressions(
    expressions: Seq[AggregateExpression]
  ): Seq[(ColumnAggregator, Seq[AttributeName])] = {
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
      case Add(_, _, _)      => AdditionAggregator(new VeArrowNativeInterfaceNumeric(
        Aurora4SparkExecutorPlugin._veo_proc, Aurora4SparkExecutorPlugin._veo_ctx,
        Aurora4SparkExecutorPlugin.lib
      ))
      case Subtract(_, _, _) => SubtractionAggregator(MultipleColumnsOffHeapSubtractor.VeoBased)
      case _                 => NoAggregationAggregator
    }
  }
}
