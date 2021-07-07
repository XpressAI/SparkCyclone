package com.nec.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import com.nec.spark.planning.NativeCsvExec
import com.nec.cmake._

final class NativeCsvExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      NativeCsvExec.NativeCsvStrategy(CNativeEvaluator)
    )
  }
}
