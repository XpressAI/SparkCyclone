package com.nec.spark.planning

import com.nec.spark.agile.AggregationExpression
import com.nec.spark.agile.AggregationFunction
import com.nec.spark.agile.AttributeName
import com.nec.spark.agile.AvgAggregation
import com.nec.spark.agile.Column
import com.nec.spark.agile.ColumnAggregationExpression
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import com.nec.spark.agile.GenericSparkPlanDescription
import com.nec.spark.agile.NoAggregationExpression
import com.nec.spark.agile.OutputColumnPlanDescription
import com.nec.spark.agile.SubtractExpression
import com.nec.spark.agile.SumAggregation
import com.nec.spark.agile.SumExpression
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.execution.SparkPlan

object VeoGenericPlanExtractor {
  def matchPlan(sparkPlan: SparkPlan): Option[GenericSparkPlanDescription] = {
    println(sparkPlan.getClass.getCanonicalName)
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
        val columnMappings = extractExpressions(exprs).zipWithIndex
          .map { case ((operation, attributes), id) =>
            ColumnAggregationExpression(
              attributes.map(attr => Column(columnIndices(attr.value), attr.value)),
              operation,
              id
            )
          }

        val outputDescription = columnMappings.zip(extractAggregationFunctions(exprs)).map {
          case (colAggregation, aggregationFunction) =>
            OutputColumnPlanDescription(
              colAggregation.columns,
              colAggregation.columnIndex,
              colAggregation.aggregation,
              aggregationFunction
            )
        }

        GenericSparkPlanDescription(fourth, outputDescription)
      }
    }
  }

  def extractExpressions(
    expressions: Seq[AggregateExpression]
  ): Seq[(AggregationExpression, Seq[AttributeName])] = {
    expressions.map {
      case AggregateExpression(sum @ Sum(expr), _, _, _, _) =>
        (extractOperation(expr), extractAttributes(sum.references))
      case AggregateExpression(avg @ Average(expr), _, _, _, _) =>
        (extractOperation(expr), extractAttributes(avg.references))
    }
  }

  def extractAttributes(attributes: AttributeSet): Seq[AttributeName] = {
    attributes.map(reference => AttributeName(reference.name)).toSeq
  }

  def extractAggregationFunctions(
    expressions: Seq[AggregateExpression]
  ): Seq[AggregationFunction] = {
    val attributeNames = expressions.map {
      case AggregateExpression(Average(_), _, _, _, _) => AvgAggregation
      case AggregateExpression(Sum(_), _, _, _, _)     => SumAggregation
    }

    attributeNames
  }
  def extractOperation(expression: Expression): AggregationExpression = {
    expression match {
      case Add(_, _, _)      => SumExpression
      case Subtract(_, _, _) => SubtractExpression
      case _                 => NoAggregationExpression
    }
  }
}
