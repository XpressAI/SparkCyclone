package com.nec.spark.planning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.{HashedRelation, UnsafeHashedRelation}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}
import org.apache.spark.sql.types.DoubleType

case class JoinPlan(
                   left: SparkPlan,
                   right: SparkPlan
                   ) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    val broadcast = if(left.isInstanceOf[BroadcastExchangeExec]) {
      left.executeBroadcast[UnsafeHashedRelation]()
    } else {
      right.executeBroadcast[InternalRow]()
    }

    null
  }

  override def output: Seq[Attribute] = Seq(
    AttributeReference(name = "_1", dataType = DoubleType, nullable = false)(),
    AttributeReference(name = "_2", dataType = DoubleType, nullable = false)()
  )

  override def children: Seq[SparkPlan] = Seq(left, right)
}
