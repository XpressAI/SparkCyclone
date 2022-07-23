package io.sparkcyclone.sql.rules

import io.sparkcyclone.spark.plans.{SparkToVectorEnginePlan, SupportsVeColBatch, VectorEngineToSparkPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

object VeColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = { case plan =>
    plan.transform {
      // TODO: Decide what to do with required sortOrders
      case SparkToVectorEnginePlan(VectorEngineToSparkPlan(child), _) => child
      case SparkToVectorEnginePlan(child: SupportsVeColBatch, _)      => child
    }
  }
}
