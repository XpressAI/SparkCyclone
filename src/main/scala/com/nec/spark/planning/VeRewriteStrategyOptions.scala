package com.nec.spark.planning

import com.nec.ve.exchange.VeExchangeStrategy
import com.nec.ve.exchange.join.VeJoinStrategy
import org.apache.spark.SparkConf

final case class VeRewriteStrategyOptions(
  aggregateOnVe: Boolean,
  enableVeSorting: Boolean,
  projectOnVe: Boolean,
  filterOnVe: Boolean,
  exchangeStrategy: Option[VeExchangeStrategy],
  passThroughProject: Boolean,
  failFast: Boolean,
  joinStrategy: Option[VeJoinStrategy]
) {}

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
      exchangeStrategy = sparkConf
        .getOption(key = "spark.com.nec.spark.exchange-strategy") match {
        case Some("none") => None
        case other        => other.flatMap(VeExchangeStrategy.fromString).orElse(default.exchangeStrategy)
      },
      passThroughProject = sparkConf
        .getOption(key = "spark.com.nec.spark.pass-through-project")
        .map(_.toBoolean)
        .getOrElse(default.passThroughProject),
      failFast = sparkConf
        .getOption(key = "spark.com.nec.spark.fail-fast")
        .map(_.toBoolean)
        .getOrElse(default.failFast),
      joinStrategy = sparkConf
        .getOption(key = "spark.com.nec.spark.join-strategy")
        .flatMap(VeJoinStrategy.fromString)
        .orElse(default.joinStrategy)
    )
  }

  val default: VeRewriteStrategyOptions =
    VeRewriteStrategyOptions(
      enableVeSorting = false,
      projectOnVe = true,
      filterOnVe = true,
      aggregateOnVe = true,
      exchangeStrategy = Option(VeExchangeStrategy.sparkShuffleBased),
      passThroughProject = false,
      failFast = false,
      joinStrategy = None
    )
}
