package com.nec.spark.planning

import com.nec.ve.VeColBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.SparkPlan

trait SupportsVeColBatch { this: SparkPlan =>
  def executeVeColumnar(): RDD[VeColBatch]
}
