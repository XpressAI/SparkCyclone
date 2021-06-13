package com.nec.spark.planning

import com.nec.spark.agile.{AttributeName, Column,
  ColumnAggregationExpression, GenericSparkPlanDescription, OutputColumnPlanDescription}

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeSet, Expression, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object VeoGenericPlanExtractor {
  def matchPlan(sparkPlan: SparkPlan): Option[GenericSparkPlanDescription] = {
    PartialFunction.condOpt(sparkPlan) {
      case first @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            exprs,
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
          ) => {
        val columnIndices = fourth.output.map(_.name).zipWithIndex.toMap
        val columnMappings = exprs.map(expression => (expression, extractAttributes(expression.references)))
          .zipWithIndex
          .map { case ((operation, attributes), id) =>
            ColumnAggregationExpression(
              attributes.map(attr => Column(columnIndices(attr.value), attr.value)),
              operation,
              id
            )
          }

        val outputDescription = columnMappings.zip(exprs).map {
          case (colAggregation, expression) =>
            OutputColumnPlanDescription(
              colAggregation.columns,
              colAggregation.columnIndex,
              colAggregation.aggregation,
              expression.aggregateFunction
            )
        }

        GenericSparkPlanDescription(fourth, outputDescription)
      }
    }
  }

  def extractAttributes(attributes: AttributeSet): Seq[AttributeName] = {
    attributes.map(reference => AttributeName(reference.name)).toSeq
  }
}
