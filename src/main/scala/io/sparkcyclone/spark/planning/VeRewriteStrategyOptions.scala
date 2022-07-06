package io.sparkcyclone.spark.planning

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
  amplifyBatches: Boolean,
  rewriteEnabled: Boolean
)

object VeRewriteStrategyOptions {
  //noinspection MapGetOrElseBoolean
  def fromConfig(sparkConf: SparkConf): VeRewriteStrategyOptions = {
    VeRewriteStrategyOptions(
      aggregateOnVe = sparkConf
        .getOption(key = "spark.cyclone.spark.aggregate-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.aggregateOnVe),
      enableVeSorting = sparkConf
        .getOption(key = "spark.cyclone.spark.sort-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.enableVeSorting),
      projectOnVe = sparkConf
        .getOption(key = "spark.cyclone.spark.project-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.projectOnVe),
      filterOnVe = sparkConf
        .getOption(key = "spark.cyclone.spark.filter-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.filterOnVe),
      exchangeOnVe = sparkConf
        .getOption(key = "spark.cyclone.spark.exchange-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.exchangeOnVe),
      passThroughProject = sparkConf
        .getOption(key = "spark.cyclone.spark.pass-through-project")
        .map(_.toBoolean)
        .getOrElse(default.passThroughProject),
      failFast = {
        sparkConf
          .getOption(key = "spark.cyclone.spark.fail-fast")
          .map(_.toBoolean)
          .getOrElse(default.failFast)
      },
      joinOnVe = sparkConf
        .getOption(key = "spark.cyclone.spark.join-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.joinOnVe),
      amplifyBatches = sparkConf
        .getOption(key = "spark.cyclone.spark.amplify-batches")
        .map(_.toBoolean)
        .getOrElse(default.amplifyBatches),
      rewriteEnabled = sparkConf
        .getOption(key = "spark.cyclone.spark.rewrite-enabled")
        .map(_.toBoolean)
        .getOrElse(default.rewriteEnabled)
    )
  }

  val default: VeRewriteStrategyOptions =
    VeRewriteStrategyOptions(
      enableVeSorting = false,
      projectOnVe = false,
      filterOnVe = false,
      aggregateOnVe = true,
      exchangeOnVe = false,
      passThroughProject = false,
      failFast = false,
      joinOnVe = false,
      amplifyBatches = true,
      rewriteEnabled = true
    )
}
