package com.nec.spark

import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.planning.VERewriteStrategy
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

object LocalVeoExtension {
  var _enabled = true
}

final class LocalVeoExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      new VERewriteStrategy(sparkSession, ExecutorPluginManagedEvaluator)
    )
  }
}
