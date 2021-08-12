package com.nec.spark

import com.nec.native.NativeCompiler
import com.nec.native.NativeEvaluator.BroadcastEvaluator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import com.nec.spark.planning.NativeCsvExec
import com.nec.native.NativeEvaluator.CNativeEvaluator

final class NativeCsvExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy { sparkSession =>
      val conf = sparkSession.sparkContext.getConf
      val selection = conf.get("spark.com.nec.native-csv", "false").toLowerCase()
      selection match {
        case "x86" => NativeCsvExec.NativeCsvStrategy(CNativeEvaluator)
        case "ve" =>
          NativeCsvExec.NativeCsvStrategy(new BroadcastEvaluator(NativeCompiler.fromConfig(conf)))
        case _ => EmptyStrategy
      }
    }
  }
}
