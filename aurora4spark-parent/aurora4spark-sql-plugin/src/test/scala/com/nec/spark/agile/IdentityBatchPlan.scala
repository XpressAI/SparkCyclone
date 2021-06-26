package com.nec.spark.agile

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.expressions.Attribute

final case class IdentityBatchPlan(child: SparkPlan) extends SparkPlan {
  protected override def doExecute(): RDD[InternalRow] =
    child
      .execute()
      .mapPartitions(iterInternalRows => iterInternalRows.map(_.copy()).toList.toIterator)

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = Seq(child)
}
