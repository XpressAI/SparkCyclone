/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.spark

import com.nec.arrow.VeArrowNativeInterface
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import com.nec.spark.SparkCycloneExecutorPlugin._
import com.nec.ve.VeProcess
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.api.plugin.ExecutorPlugin
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging

import java.nio.file.Files
import java.nio.file.Path
import scala.util.Try

object SparkCycloneExecutorPlugin {

  /** For assumption testing purposes only for now */
  var params: Map[String, String] = Map.empty[String, String]

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
  var _veo_proc: veo_proc_handle = _

  implicit def veProcess: VeProcess =
    VeProcess.DeferredVeProcess(() => VeProcess.WrappingVeo(_veo_proc))

  var lib: Long = -1
  var veArrowNativeInterfaceNumeric: VeArrowNativeInterface = _

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
    if (_veo_proc != null) {
      veo.veo_proc_destroy(_veo_proc)
    }
  }

  var DefaultVeNodeId = 0

  trait LibraryStorage {
    // Get a local copy of the library for loading
    def getLocalLibraryPath(code: String): Path
  }

  final class DriverFetchingLibraryStorage(pluginContext: PluginContext)
    extends LibraryStorage
    with LazyLogging {

    private var locallyStoredLibs = Map.empty[String, Path]

    /** Get a local copy of the library for loading */
    override def getLocalLibraryPath(code: String): Path = this.synchronized {
      locallyStoredLibs.get(code) match {
        case Some(result) =>
          logger.debug("Cache hit for executor-fetch for code.")
          result
        case None =>
          logger.debug("Cache miss for executor-fetch for code; asking Driver.")
          val result = pluginContext.ask(RequestCompiledLibraryForCode(code))
          if (result == null) {
            sys.error(s"Could not fetch library: ${code}")
          } else {
            val localPath = Files.createTempFile("ve_fn", ".lib")
            Files.write(
              localPath,
              result.asInstanceOf[RequestCompiledLibraryResponse].byteString.toByteArray
            )
            logger.debug(s"Saved file to '$localPath'")
            locallyStoredLibs += code -> localPath
            localPath
          }
      }
    }
  }

  var libraryStorage: LibraryStorage = _
}

class SparkCycloneExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    val resources = ctx.resources()
    SparkCycloneExecutorPlugin.synchronized {
      SparkCycloneExecutorPlugin.libraryStorage = new DriverFetchingLibraryStorage(ctx)
    }

    logInfo(s"Executor has the following resources available => ${resources}")
    val selectedVeNodeId = if (!resources.containsKey("ve")) {
      val nodeId = Try(System.getenv("VE_NODE_NUMBER").toInt).getOrElse(DefaultVeNodeId)
      logInfo(s"Do not have a VE resource available. Will use '${nodeId}' as the main resource.")
      nodeId
    } else {
      val veResources = resources.get("ve")
      val resourceCount = veResources.addresses.length
      val executorNumber = Try(ctx.executorID().toInt - 1).getOrElse(0) // Executor IDs start at 1.
      val veMultiple = executorNumber / 8
      if (veMultiple > resourceCount) {
        logWarning("Not enough VE resources allocated for number of executors specified.")
      }
      veResources.addresses(veMultiple % resourceCount).toInt
    }

    logInfo(s"Using VE node = ${selectedVeNodeId}")

    if (_veo_proc == null) {
      _veo_proc = veo.veo_proc_create(selectedVeNodeId)
      require(
        _veo_proc != null,
        s"Proc could not be allocated for node ${selectedVeNodeId}, got null"
      )
      require(_veo_proc.address() != 0, s"Address for 0 for proc was ${_veo_proc}")
      logInfo(s"Opened process: ${_veo_proc}")

      /**
       * We currently do two approaches - one is to pre-compile, and another is to compile at the point of the SQL.
       * We're moving to the latter, however this is retained for compatibility for the previous set of sets we had.
       * *
       */
      if (extraConf.containsKey("ve_so_name")) {
        SparkCycloneExecutorPlugin.lib =
          veo.veo_load_library(_veo_proc, extraConf.get("ve_so_name"))
      }
      veArrowNativeInterfaceNumeric = new VeArrowNativeInterface(_veo_proc, lib)
    }
    logInfo("Initializing SparkCycloneExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  override def shutdown(): Unit = {
    if (closeAutomatically) {
      logInfo(s"Closing process: ${_veo_proc}")
      closeProcAndCtx()
    }
    super.shutdown()
  }
}
