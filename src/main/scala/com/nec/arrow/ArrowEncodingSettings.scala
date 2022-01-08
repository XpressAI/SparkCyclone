package com.nec.arrow

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf

/** This is done as [[SqlConf]] cannot be serialized. */
final case class ArrowEncodingSettings(timezone: String, numRows: Int)

object ArrowEncodingSettings {
  def fromConf(conf: SQLConf)(implicit sparkContext: SparkContext): ArrowEncodingSettings =
    ArrowEncodingSettings(
      timezone =
        try conf.sessionLocalTimeZone
        catch {
          case _: Throwable => "UTC"
        },
      numRows = sparkContext.getConf
        .getOption("com.nec.spark.ve.columnBatchSize")
        .map(_.toInt)
        .getOrElse(conf.columnBatchSize)
    )
}
