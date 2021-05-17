package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeSet, Expression, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Sum}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
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
            fourth @ LocalTableScanExec(output, rows)
          ),
          shuffleOrigin
        )
      ) => {
        val columnIndices = output.map(_.name).zipWithIndex.toMap
        val columnMappings = extractExpressions(exprs).zipWithIndex
          .map{
            case ((operation, attributes), id) => ColumnAggregation(
              attributes.map(attr => Column(columnIndices(attr.value), attr.value)),
              operation,
              id
            )
          }

        val outputDescription = columnMappings.zip(extractAggregationFunctions(exprs)).map{
          case (colAggregation, aggregationFunction) =>
            OutputColumnPlanDescription(colAggregation.columns, colAggregation.columnIndex,
              colAggregation.aggregation, aggregationFunction)
        }

        GenericSparkPlanDescription(fourth, outputDescription)
      }
    }
  }

  def extractExpressions(expressions: Seq[AggregateExpression]):
    Seq[(ColumnAggregateOperation, Seq[AttributeName])] = {
      expressions.map {
        case AggregateExpression(sum @ Sum(expr), _, _, _, _) =>
          (extractOperation(expr), extractAttributes(sum.references))
        case AggregateExpression(avg @ Average(expr), _, _, _, _) =>
          (extractOperation(expr), extractAttributes(avg.references))
      }
  }

  def extractAttributes(attributes: AttributeSet): Seq[AttributeName] = {
    attributes.map(reference => AttributeName(reference.name))
      .toSeq
  }

  def extractAggregationFunctions(expressions: Seq[AggregateExpression]):
  Seq[AggregationFunction] = {
    val attributeNames = expressions.map {
      case AggregateExpression(Average(_), _, _, _, _) => AvgAggregation
      case AggregateExpression(Sum(_), _, _, _, _) => SumAggregation
    }

    attributeNames
  }
  def extractOperation(expression: Expression): ColumnAggregateOperation = {
    expression match {
      case Add(_, _, _) => Addition
      case Subtract(_, _, _) => Subtraction
      case _ => NoAggregation
    }
  }
}
