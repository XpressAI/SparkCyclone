package com.nec.spark

import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.planning.VERewriteStrategy
import com.nec.spark.planning.VERewriteStrategy.VeRewriteStrategyOptions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

object LocalVeoExtension {
  var _enabled = true
}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      new VERewriteStrategy(
        ExecutorPluginManagedEvaluator,
        VeRewriteStrategyOptions(
          preShufflePartitions = sparkSession.sparkContext.getConf
            .getOption(key = "spark.com.nec.spark.preshuffle-partitions")
            .map(_.toInt),
          sparkSession.sparkContext.getConf
            .getOption(key = "spark.com.nec.spark.sort-on-ve")
            .map(_.toBoolean)
            .getOrElse(false)
        )
      )
    )
  }
}
