package com.nec.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import com.nec.spark.planning.NativeCsvExec
import com.nec.native.NativeEvaluator.CNativeEvaluator
import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator

final class NativeCsvExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy { sparkSession =>
      val conf = sparkSession.sparkContext.getConf
      val selection = conf.get("spark.com.nec.native-csv", "false").toLowerCase()
      selection match {
        case "x86" => NativeCsvExec.NativeCsvStrategy(CNativeEvaluator)
        case "ve"  => NativeCsvExec.NativeCsvStrategy(ExecutorPluginManagedEvaluator)
        case _     => EmptyStrategy
      }
    }
  }
}
