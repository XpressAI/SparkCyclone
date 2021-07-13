package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.{Float8VectorWrapper, IntVectorWrapper}
import org.apache.arrow.vector.{Float8Vector, IntVector}

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Sum => SparkSum}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}

object GroupBySum {

  val GroupBySumSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/com/nec/arrow/functions/cpp/grouper.cc"))
    try source.mkString
    finally source.close()
  }

  def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
    groupingVector: Float8Vector,
    valuesVector: Float8Vector,
    outputGroupsVector: Float8Vector,
    outputValuesVector: Float8Vector
  ): Unit = {

    nativeInterface.callFunction(
      name = "group_by_sum",
      inputArguments = List(
        Some(Float8VectorWrapper(groupingVector)),
        Some(Float8VectorWrapper(valuesVector)),
        None,
        None,
      ),
      outputArguments = List(
        None,
        None,
        Some(Float8VectorWrapper(outputValuesVector)),
        Some(Float8VectorWrapper(outputGroupsVector)),
      )
    )
  }

  def groupBySumJVM(groupingVector: Float8Vector, valuesVector: Float8Vector): Map[Double, Double] = {
    val groupingVals = (0 until groupingVector.getValueCount).map(idx => groupingVector.get(idx))
    val valuesVals = (0 until valuesVector.getValueCount).map(idx => valuesVector.get(idx))

    groupingVals
      .zip(valuesVals)
      .groupBy(_._1)
      .map(kv => (kv._1, kv._2.map(_._2).sum))

  }

  def isLogicalGroupBySum(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case agg @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
        val isSum = aggregateExpressions.collect {
          case agg @ Alias(
            AggregateExpression(SparkSum(expr), mode, isDistinct, filter, resultId),
            name) => agg
        }.size == 1

        groupingExpressions.size == 1 && aggregateExpressions.size == 2 &&
        aggregateExpressions.contains(groupingExpressions.head) && isSum

      case _ => false
    }
  }
}
