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
        .getOption(key = "spark.cyclone.sql.aggregate-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.aggregateOnVe),
      enableVeSorting = sparkConf
        .getOption(key = "spark.cyclone.sql.sort-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.enableVeSorting),
      projectOnVe = sparkConf
        .getOption(key = "spark.cyclone.sql.project-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.projectOnVe),
      filterOnVe = sparkConf
        .getOption(key = "spark.cyclone.sql.filter-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.filterOnVe),
      exchangeOnVe = sparkConf
        .getOption(key = "spark.cyclone.sql.exchange-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.exchangeOnVe),
      passThroughProject = sparkConf
        .getOption(key = "spark.cyclone.sql.pass-through-project")
        .map(_.toBoolean)
        .getOrElse(default.passThroughProject),
      failFast = {
        sparkConf
          .getOption(key = "spark.cyclone.sql.fail-fast")
          .map(_.toBoolean)
          .getOrElse(default.failFast)
      },
      joinOnVe = sparkConf
        .getOption(key = "spark.cyclone.sql.join-on-ve")
        .map(_.toBoolean)
        .getOrElse(default.joinOnVe),
      amplifyBatches = sparkConf
        .getOption(key = "spark.cyclone.sql.amplify-batches")
        .map(_.toBoolean)
        .getOrElse(default.amplifyBatches),
      rewriteEnabled = sparkConf
        .getOption(key = "spark.cyclone.sql.rewrite-enabled")
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
