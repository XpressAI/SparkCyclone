package com.nec.spark.planning.plans

import com.nec.spark.planning.SupportsVeColBatch
import com.nec.spark.planning.SupportsVeColBatch.DataCleanup
import com.nec.spark.planning.VeColBatchConverters.{getNumRows, internalRowToVeColBatch}
import com.nec.ve.VeColBatch
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class SparkToVectorEnginePlan(childPlan: SparkPlan)
  extends UnaryExecNode
  with LazyLogging
  with SupportsVeColBatch {

  override protected def doCanonicalize(): SparkPlan = {
    super.doCanonicalize()
  }

  override def child: SparkPlan = {
    childPlan
  }

  override def executeVeColumnar(): RDD[VeColBatch] = {
    require(!child.isInstanceOf[SupportsVeColBatch], "Child should not be a VE plan")

    //      val numInputRows = longMetric("numInputRows")
    //      val numOutputBatches = longMetric("numOutputBatches")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows: Int = getNumRows(sparkContext, conf)
    logger.info(s"Will make batches of ${numRows} rows...")
    val timeZoneId = conf.sessionLocalTimeZone

    internalRowToVeColBatch(child.execute(), timeZoneId, child.schema, numRows)
  }

  override def output: Seq[Attribute] = child.output

  override def dataCleanup: DataCleanup = DataCleanup.Cleanup

}
