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

import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import com.nec.ve.VeProcess.LibraryReference
import com.nec.ve.{VeColBatch, VeProcess}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkEnv
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.ProcessExecutorMetrics
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Try

object SparkCycloneExecutorPlugin extends LazyLogging {

  /** For assumption testing purposes only for now */
  var params: Map[String, String] = Map.empty[String, String]

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
  var _veo_proc: veo_proc_handle = _
  @transient val libsPerProcess: scala.collection.mutable.Map[
    veo_proc_handle,
    scala.collection.mutable.Map[String, LibraryReference]
  ] =
    scala.collection.mutable.Map.empty

  implicit def veProcess: VeProcess =
    VeProcess.DeferredVeProcess(() => VeProcess.WrappingVeo(_veo_proc, source, metrics))

  implicit def source: VeColVectorSource = VeColVectorSource(
    s"Process ${_veo_proc}, executor ${SparkEnv.get.executorId}"
  )

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
  var CloseAutomatically: Boolean = true
  def closeProcAndCtx(): Unit = {
    if (_veo_proc != null) {
      veo.veo_proc_destroy(_veo_proc)
    }
  }

  var DefaultVeNodeId = 0

  @transient val cachedBatches: scala.collection.mutable.Set[VeColBatch] =
    scala.collection.mutable.Set.empty

  @transient val cachedCols: scala.collection.mutable.Set[VeColVector] =
    scala.collection.mutable.Set.empty

  def cleanCache(): Unit = {
    cachedBatches.toList.foreach { colBatch =>
      cachedBatches.remove(colBatch)
      colBatch.cols.foreach(freeCol)
    }
  }

  def freeCol(col: VeColVector): Unit = {
    if (cachedCols.contains(col)) {
      logger.debug(s"Freeing column ${col}... in ${source}")
      cachedCols.remove(col)
      col.free()
    }
  }

  def register(cb: VeColBatch): Unit = {
    cachedBatches.add(cb)
    cb.cols.foreach(cachedCols.add)
  }

  var CleanUpCache: Boolean = true

  def cleanUpIfNotCached(veColBatch: VeColBatch): Unit =
    if (!cachedBatches.contains(veColBatch))
      veColBatch.cols.filterNot(cachedCols.contains).foreach(freeCol)

  val metrics = new ProcessExecutorMetrics()
}

class SparkCycloneExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    import com.nec.spark.SparkCycloneExecutorPlugin._

    SparkEnv.get.metricsSystem.registerSource(SparkCycloneExecutorPlugin.metrics)
    val resources = ctx.resources()

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
    }
    logInfo("Initializing SparkCycloneExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  override def shutdown(): Unit = {
    if (SparkCycloneExecutorPlugin.CleanUpCache) {
      SparkCycloneExecutorPlugin.cleanCache()
    }

    import com.nec.spark.SparkCycloneExecutorPlugin._
    Option(metrics.getAllocations)
      .filter(_.nonEmpty)
      .foreach(unfinishedAllocations =>
        logger.error(s"There were some unreleased allocations: ${unfinishedAllocations}, expected to be none.")
      )

    if (CloseAutomatically) {
      logInfo(s"Closing process: ${_veo_proc}")
      closeProcAndCtx()
    }
    super.shutdown()
  }
}
