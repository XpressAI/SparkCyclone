package com.nec.spark
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object EmptyStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = Nil
}
