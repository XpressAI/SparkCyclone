package com.nec.spark.agile

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.BlockingOperatorWithCodegen
import org.apache.spark.sql.execution.SparkPlan

final case class IdentityCodegenBatchPlan(child: SparkPlan)
  extends SparkPlan
  with BlockingOperatorWithCodegen
  with UnsafeExternalProcessorBase {
  type ContainerType = UnsafeExternalDuplicator
  def createContainer(): UnsafeExternalDuplicator = new UnsafeExternalDuplicator
  override def containerClass: Class[UnsafeExternalDuplicator] = classOf[UnsafeExternalDuplicator]
  override def output: Seq[Attribute] = child.output
  override def children: Seq[SparkPlan] = Seq(child)
}
