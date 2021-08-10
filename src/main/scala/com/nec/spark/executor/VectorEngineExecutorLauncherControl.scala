package com.nec.spark.executor
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.executor.VectorEngineExecutorLauncherControl.staticPlugin

import java.util

object VectorEngineExecutorLauncherControl {
  lazy val staticPlugin = new Aurora4SparkExecutorPlugin()
}
class VectorEngineExecutorLauncherControl extends SparkNecExecutorControl {
  override def init(): Unit = staticPlugin.init(new util.HashMap[String, String]())
  override def stop(): Unit = staticPlugin.shutdown()
}
