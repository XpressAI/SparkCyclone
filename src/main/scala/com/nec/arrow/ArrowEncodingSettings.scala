package com.nec.arrow

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf

/** This is done as [[SqlConf]] cannot be serialized. */
final case class ArrowEncodingSettings(timezone: String, numRows: Int, batchSizeTargetBytes: Int)

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
        .getOrElse(conf.columnBatchSize),
      batchSizeTargetBytes = sparkContext.getConf
        .getOption("com.nec.spark.ve.targetBatchSizeKb")
        .map(_.toInt)
        .map(_ * 1024)
        .getOrElse(DefaultTargetBatchSize)
    )

  // 64M
  private val DefaultTargetBatchSize: Int = 64 * 1024 * 1024
}
