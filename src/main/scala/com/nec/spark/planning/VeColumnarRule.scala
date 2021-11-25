package com.nec.spark.planning

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

class VeColumnarRule extends ColumnarRule {
  override def preColumnarTransitions: Rule[SparkPlan] = sparkPlan => {
    sparkPlan.transform {
      case RowToArrowColumnarPlan(ArrowColumnarToRowPlan(child)) => child
    }
  }

}