package com.nec.spark.planning

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf

object VeColBatchConverters {

  def getNumRows(sparkContext: SparkContext, conf: SQLConf): Int = {
    sparkContext.getConf
      .getOption("com.nec.spark.ve.columnBatchSize")
      .map(_.toInt)
      .getOrElse(conf.columnBatchSize)
  }

}
