package com.nec.spark.driver

import com.nec.spark.Aurora4SparkDriverPlugin

import java.util
import org.apache.spark.SparkContext

object VectorEngineDriverLauncherControl {
  lazy val staticPlugin: Aurora4SparkDriverPlugin = new Aurora4SparkDriverPlugin()
}

class VectorEngineExecutorLauncherControl extends SparkNecDriverControl {

  override def init(): Unit = VectorEngineDriverLauncherControl.staticPlugin.init(SparkContext.getOrCreate())
  override def stop(): Unit = VectorEngineDriverLauncherControl.staticPlugin.shutdown()
}
