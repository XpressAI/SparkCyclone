package com.nec.spark

import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.aurora.Aurora
import com.nec.aurora.Aurora.veo_proc_handle

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import com.nec.spark.Aurora4SparkExecutorPlugin._
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

  /**
   * https://www.hpc.nec/documents/veos/en/veoffload/md_Restriction.html
   *
   * VEO does not support:
   * to use quadruple precision real number a variable length character string as a return value and an argument of Fortran subroutines and functions,
   * to use multiple VEs by a VH process,
   * to re-create a VE process after the destruction of the VE process, and
   * to call API of VE DMA or VH-VE SHM on the VE side if VEO API is called from child thread on the VH side.
   *
   * *
   */
  var closeAutomatically: Boolean = false
  def closeProcAndCtx(): Unit = {
    Aurora.veo_context_close(_veo_ctx)
    Aurora.veo_proc_destroy(_veo_proc)
  }
}

class Aurora4SparkExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {

    logError(s"${ctx.executorID()}")
    logError(s"Resources => ${ctx.resources}")

    if (_veo_proc == null) {
      _veo_proc = Aurora.veo_proc_create(0)
      _veo_ctx = Aurora.veo_context_open(_veo_proc)

      /**
       * We currently do two approaches - one is to pre-compile, and another is to compile at the point of the SQL.
       * We're moving to the latter, however this is retained for compatibility for the previous set of sets we had.
       * *
       */
      if (extraConf.containsKey("ve_so_name")) {
        Aurora4SparkExecutorPlugin.lib =
          Aurora.veo_load_library(_veo_proc, extraConf.get("ve_so_name"))
      }
      veArrowNativeInterface = new VeArrowNativeInterface(_veo_proc, _veo_ctx, lib)
      veArrowNativeInterfaceNumeric = new VeArrowNativeInterfaceNumeric(_veo_proc, _veo_ctx, lib)
    }
    logInfo("Initializing Aurora4SparkExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  override def shutdown(): Unit = {
    if (closeAutomatically) {
      closeProcAndCtx()
    }
    super.shutdown()
  }
}
