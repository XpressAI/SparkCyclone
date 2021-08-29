package com.nec.spark.driver

import com.nec.spark.Aurora4SparkDriverPlugin

import java.util
import org.apache.spark.SparkContext

object VectorEngineDriverLauncherControl {
}

class VectorEngineDriverLauncherControl extends SparkNecDriverControl {

  override def init(obj: Object): Unit = Aurora4SparkDriverPlugin.injectVeoExtension(obj)
}
