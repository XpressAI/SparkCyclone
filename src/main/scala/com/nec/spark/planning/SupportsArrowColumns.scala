package com.nec.spark.planning

import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

trait SupportsArrowColumns {
  self: UnaryExecNode =>
  def getChildSkipMappings(): SparkPlan = {
    self.child match {
      case ArrowColumnarToRowPlan(RowToArrowColumnarPlan(c)) => c
      case other                                             => other
    }
  }
}
