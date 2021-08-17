package com.nec.spark

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

package object agile {
  case class AttributeName(value: String) extends AnyVal
  case class SingleColumnSparkPlan(sparkPlan: SparkPlan, column: Column)
  case class PartialSingleColumnSparkPlan(
    plans: Seq[SparkPlan],
    parent: SparkPlan,
    child: SparkPlan,
    column: Column
  ) {
    def replaceMain(replaceWith: SparkPlan): SparkPlan = {
      plans.reverse
        .dropWhile(plan => plan != parent)
        .foldLeft(replaceWith) {
          case (plan, f @ HashAggregateExec(_, _, _, _, _, _, _)) => {
            f.copy(child = plan)
          }
          case (plan, f @ ShuffleExchangeExec(_, _, _)) => {
            f.copy(child = plan)
          }
        }
    }
  }

  case class GenericSparkPlanDescription(
    sparkPlan: SparkPlan,
    outColumns: Seq[OutputColumnPlanDescription]
  )

  type ColumnIndex = Int
  type ColumnWithNumbers = (ColumnIndex, Iterable[Double])

  case class ColumnAggregationExpression(
    columns: Seq[Column],
    aggregation: Expression,
    columnIndex: ColumnIndex
  ) extends Serializable

  case class Column(index: Int, name: String) extends Serializable

  case class OutputColumnPlanDescription(
    inputColumns: Seq[Column],
    outputColumnIndex: ColumnIndex,
    columnAggregation: Expression,
    outputAggregator: AggregateFunction
  ) extends Serializable

}
