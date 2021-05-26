package com.nec.spark

import com.nec.aurora.Aurora
import com.nec.aurora.Aurora.veo_proc_handle
import com.nec.native.{VeArrowNativeInterface, VeArrowNativeInterfaceNumeric}
import java.util

import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.nec.spark.Aurora4SparkExecutorPlugin._
import com.nec.spark.LocalVeoExtension.ve_so_name

import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

object Aurora4SparkExecutorPlugin {

  /** For assumption testing purposes only for now */
  var params: Map[String, String] = Map.empty[String, String]

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
  var _veo_proc: veo_proc_handle = _
  var _veo_ctx: Aurora.veo_thr_ctxt = _
  var lib: Long = -1
  var veArrowNativeInterface: VeArrowNativeInterface = _
  var veArrowNativeInterfaceNumeric: VeArrowNativeInterfaceNumeric = _
}

class Aurora4SparkExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    _veo_proc = Aurora.veo_proc_create(0)
    _veo_ctx = Aurora.veo_context_open(_veo_proc)
    Aurora4SparkExecutorPlugin.lib = Aurora.veo_load_library(_veo_proc, ve_so_name.toString)
    veArrowNativeInterface =
      new VeArrowNativeInterface(_veo_proc, _veo_ctx, Aurora4SparkExecutorPlugin.lib)
    veArrowNativeInterfaceNumeric = new VeArrowNativeInterfaceNumeric(_veo_proc,
      _veo_ctx, Aurora4SparkExecutorPlugin.lib)
    logInfo("Initializing Aurora4SparkExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  override def shutdown(): Unit = {
    Aurora.veo_context_close(_veo_ctx)
    Aurora.veo_proc_destroy(_veo_proc)
    super.shutdown()
  }
}
