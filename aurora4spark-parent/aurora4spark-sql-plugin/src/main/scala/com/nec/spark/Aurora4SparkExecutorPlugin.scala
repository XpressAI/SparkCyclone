package com.nec.spark

import com.nec.aurora.Aurora
import com.nec.aurora.Aurora.veo_proc_handle

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import com.nec.spark.Aurora4SparkExecutorPlugin.{_veo_proc, launched, params}
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

object Aurora4SparkExecutorPlugin {

  /** For assumption testing purposes only for now */
  var params: Map[String, String] = Map.empty[String, String]

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
  var _veo_proc: veo_proc_handle = _
}

class Aurora4SparkExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    _veo_proc = Aurora.veo_proc_create(0)
    logInfo("Initializing Aurora4SparkExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  override def shutdown(): Unit = {
    Aurora.veo_proc_destroy(_veo_proc)
    super.shutdown()
  }
}
