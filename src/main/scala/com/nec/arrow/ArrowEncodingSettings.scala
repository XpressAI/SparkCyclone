package com.nec.arrow

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.SQLConf

/** This is done as [[SqlConf]] cannot be serialized. */
final case class ArrowEncodingSettings(timezone: String, numRows: Int, batchSizeTargetBytes: Long)

object ArrowEncodingSettings {
  def fromConf(conf: SQLConf)(implicit sparkContext: SparkContext): ArrowEncodingSettings = {

    ArrowEncodingSettings(
      timezone =
        try conf.sessionLocalTimeZone
        catch {
          case _: Throwable => "UTC"
        },
      numRows = sparkContext.getConf
        .getOption("spark.com.nec.spark.ve.columnBatchSize")
        .map(_.toInt)
        .getOrElse(conf.columnBatchSize),
      batchSizeTargetBytes = sparkContext.getConf
        .getOption("spark.com.nec.spark.ve.targetBatchSizeMb")
        .map(_.toLong)
        .map(_ * Mb)
        .getOrElse(DefaultTargetBatchSize)
    )
  }

  private val Mb: Long = 1024 * 1024

  // 64M -- trying 128M does not change performance [2022-02-11]
  private val DefaultTargetBatchSize: Long = 64 * Mb

}
