package com.nec.spark.agile

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

object CountPlanner {
  def transformPlan(sparkPlan: SparkPlan): Option[SparkPlan] =
    PartialFunction.condOpt(sparkPlan) {
      case hae @ HashAggregateExec(
            requiredChildDistributionExpressions,
            groupingExpressions,
            Seq(AggregateExpression(Count(_), mode, isDistinct, filter, resultId)),
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            ShuffleExchangeExec(
              outputPartitioning,
              HashAggregateExec(
                requiredChildDistributionExpressions2,
                groupingExpressions2,
                aggregateExpressions2,
                aggregateAttributes2,
                initialInputBufferOffset2,
                resultExpressions2,
                child
              ),
              shuffleOrigin
            )
          )
          if groupingExpressions2 == groupingExpressions && groupingExpressions.size == 1 && child.schema.head.dataType == StringType =>
        CountPlanner(child, resultExpressions.map(_.toAttribute))
    }

  def apply(sparkPlan: SparkPlan): SparkPlan = {
    sparkPlan.transform(Function.unlift(transformPlan))
  }
}

case class CountPlanner(childPlan: SparkPlan, output: Seq[Attribute]) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    childPlan
      .execute()
      .map { ir => ir.getString(0) -> (1: java.lang.Long) }
      .reduceByKey(_ + _)
      .map { case (v, c) =>
        new GenericInternalRow(Array[Any](UTF8String.fromString(v), c))
      }
  }

  override def children: Seq[SparkPlan] = Seq(childPlan)

}
