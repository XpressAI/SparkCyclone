package io.sparkcyclone.spark.planning.hints

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

sealed abstract class VeHint(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class SortOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class ProjectOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class FilterOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class AggregateOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class ExchangeOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class FailFast(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class JoinOnVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class AmplifyBatches(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
case class SkipVe(child: LogicalPlan, enabled: Boolean) extends VeHint(child)
