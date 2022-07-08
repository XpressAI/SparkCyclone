package io.sparkcyclone.sql.rules

import io.sparkcyclone.spark.planning.SupportsVeColBatch
import io.sparkcyclone.spark.planning.plans.{SparkToVectorEnginePlan, VectorEngineToSparkPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

object VeColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = { case plan =>
    plan.transform {
      // TODO: Decide what to do with required sortOrders
      case SparkToVectorEnginePlan(VectorEngineToSparkPlan(child), _, _) => child
      case SparkToVectorEnginePlan(child: SupportsVeColBatch, _, _)      => child
    }
  }
}
