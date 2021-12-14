package com.nec.spark.planning

import org.apache.spark.SparkConf

final case class VeRewriteStrategyOptions(
  aggregateOnVe: Boolean,
  enableVeSorting: Boolean,
  projectOnVe: Boolean,
  filterOnVe: Boolean,
  exchangeOnVe: Boolean
)
object VeRewriteStrategyOptions {
  //noinspection MapGetOrElseBoolean
  def fromConfig(sparkConf: SparkConf): VeRewriteStrategyOptions = {
    VeRewriteStrategyOptions(
      aggregateOnVe = sparkConf
        .getOption(key = "spark.com.nec.spark.aggregate-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.aggregateOnVe),
      enableVeSorting = sparkConf
        .getOption(key = "spark.com.nec.spark.sort-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.enableVeSorting),
      projectOnVe = sparkConf
        .getOption(key = "spark.com.nec.spark.project-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.projectOnVe),
      filterOnVe = sparkConf
        .getOption(key = "spark.com.nec.spark.filter-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.filterOnVe),
      exchangeOnVe = sparkConf
        .getOption(key = "spark.com.nec.spark.exchange-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.exchangeOnVe)
    )
  }

  val default: VeRewriteStrategyOptions =
    VeRewriteStrategyOptions(
      enableVeSorting = false,
      projectOnVe = true,
      filterOnVe = true,
      aggregateOnVe = true,
      exchangeOnVe = true
    )
}
