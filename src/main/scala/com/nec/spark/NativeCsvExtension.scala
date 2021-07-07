package com.nec.spark

final class NativeCsvExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(sparkSessionExtensions: SparkSessionExtensions): Unit = {
    sparkSessionExtensions.injectPlannerStrategy(sparkSession =>
      NativeCsvStrategy(CNativeEvaluator)
    )
  }
}
