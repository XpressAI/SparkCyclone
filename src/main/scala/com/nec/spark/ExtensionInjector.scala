package com.nec.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSessionExtensions

final class ExtensionInjector() {}

object ExtensionInjector extends LazyLogging {
  import net.bytebuddy.asm.Advice
  @Advice.OnMethodEnter
  def enter(@Advice.This obj: Object): Unit = {
    logger.info("Attaching VEO Extension")

    Aurora4SparkDriverPlugin.injectVeoExtension(
      obj
        .asInstanceOf[SparkSessionExtensions]
    )

    logger.info("Attached VEO Extension")
  }
}
