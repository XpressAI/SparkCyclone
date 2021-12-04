package com.nec.spark.planning

import com.nec.ve.VeColBatch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CachedVeRelation(
                         outputs: List[Attribute],
                         numPartitions: Int
                    ) extends LeafExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = throw new RuntimeException("We don't do that here")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new RuntimeException("We don't do that here")
  }

  def executeVe(): RDD[VeColBatch] = {
    sparkContext.parallelize(CacheManager.getCachedTable(outputs), numPartitions)
  }

  override def output: Seq[Attribute] = outputs
}


