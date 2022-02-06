package com.nec.spark.planning

import org.apache.spark.SparkConf

final case class VeRewriteStrategyOptions(
  aggregateOnVe: Boolean,
  enableVeSorting: Boolean,
  projectOnVe: Boolean,
  filterOnVe: Boolean,
  exchangeOnVe: Boolean,
  passThroughProject: Boolean,
  failFast: Boolean,
  joinOnVe: Boolean,
  amplifyBatches: Boolean
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
        .getOrElse(default.exchangeOnVe),
      passThroughProject = sparkConf
        .getOption(key = "spark.com.nec.spark.pass-through-project")
        .map(_.toBoolean)
        .getOrElse(default.passThroughProject),
      failFast = {
        sparkConf
          .getOption(key = "spark.com.nec.spark.fail-fast")
          .map(_.toBoolean)
          .getOrElse(default.failFast)
      },
      joinOnVe = sparkConf
        .getOption(key = "spark.com.nec.spark.join-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.joinOnVe),
      amplifyBatches = sparkConf
        .getOption(key = "spark.com.nec.spark.amplify-batches")
        .map(_.toBoolean)
        .getOrElse(default.amplifyBatches)
    )
  }

  val default: VeRewriteStrategyOptions =
    VeRewriteStrategyOptions(
      enableVeSorting = false,
      projectOnVe = true,
      filterOnVe = true,
      aggregateOnVe = true,
      /** Disabled by default as there appears to be a fault here **/
      exchangeOnVe = true,
      passThroughProject = false,
      failFast = false,
      joinOnVe = false,
      amplifyBatches = true
    )
}
