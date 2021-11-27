package com.nec.spark.planning

import com.nec.ve.VeColBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

trait SupportsVeColBatch { this: SparkPlan =>
  def executeVeColumnar(): RDD[VeColBatch]
  override protected def doExecute(): RDD[InternalRow] =
    sys.error("Cannot execute plan without a VE wrapper")
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    sys.error("Cannot execute plan without a VE wrapper")
}
