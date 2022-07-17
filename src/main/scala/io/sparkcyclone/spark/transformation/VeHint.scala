package io.sparkcyclone.spark.transformation

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

sealed abstract class VeHint(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class SortOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class ProjectOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class FilterOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class AggregateOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class ExchangeOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class FailFast(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class JoinOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class AmplifyBatches(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}

case class SkipVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child) {
  override def withNewChildInternal(newChild: LogicalPlan): VeHint = copy(child = newChild)
}
