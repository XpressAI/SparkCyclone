package com.nec.spark.planning

import com.nec.spark.planning.VeColBatchConverters.{SparkToVectorEngine, VectorEngineToSpark}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

final class VeColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = { case plan =>
    plan.transform {
      case SparkToVectorEngine(VectorEngineToSpark(child)) => child
      case SparkToVectorEngine(child: SupportsVeColBatch)  => child
    }
  }

}
