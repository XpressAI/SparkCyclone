package io.sparkcyclone.sql.rules

import io.sparkcyclone.spark.planning.hints._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.BooleanType

object VeHintResolutionRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp { case (h: UnresolvedHint) =>
      (h.name, h.parameters) match {
        case ("SORT_ON_VE", Seq(Literal(bool: Boolean, BooleanType))) => SortOnVe(h.child, bool)
        case ("PROJECT_ON_VE", Seq(Literal(bool: Boolean, BooleanType))) => ProjectOnVe(h.child, bool)
        case ("FILTER_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> FilterOnVe(h.child, bool)
        case ("AGGREGATE_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> AggregateOnVe(h.child, bool)
        case ("EXCHANGE_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> ExchangeOnVe(h.child, bool)
        case ("FAIL_FAST", Seq(Literal(bool: Boolean, BooleanType)))=> FailFast(h.child, bool)
        case ("JOIN_ON_VE", Seq(Literal(bool: Boolean, BooleanType)))=> JoinOnVe(h.child, bool)
        case ("AMPLIFY_BATCHES", Seq(Literal(bool: Boolean, BooleanType)))=> AmplifyBatches(h.child, bool)
        case ("SKIP_VE", Seq(Literal(bool: Boolean, BooleanType)))=> SkipVe(h.child, bool)
      }
    }
  }
}
