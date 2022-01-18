package com.nec.spark.planning

import com.nec.ve.VeColBatch
import org.apache.spark.rdd.RDD

trait SupportsKeyedVeColBatch { this: SupportsVeColBatch =>
  def executeVeColumnar(): RDD[VeColBatch] = executeVeColumnarKeyed().map(_._2)
  def executeVeColumnarKeyed(): RDD[(Int, VeColBatch)]
}
