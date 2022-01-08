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

import com.nec.spark.SparkCycloneExecutorPlugin.{launched, params, DefaultVeNodeId}
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import com.nec.ve.VeProcess.LibraryReference
import com.nec.ve.{VeColBatch, VeProcess}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkEnv
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.ProcessExecutorMetrics
import org.apache.spark.metrics.source.ProcessExecutorMetrics.AllocationTracker
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

  def freeCol(
    col: VeColVector
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Unit = {
    if (cachedCols.contains(col)) {
      logger.debug(s"Freeing column ${col}... in ${source}; from ${fullName.value}#${line.value}")
      cachedCols.remove(col)
      col.free()
    }
  }

  def registerCachedBatch(cb: VeColBatch): Unit = {
    cachedBatches.add(cb)
    cb.cols.foreach(cachedCols.add)
  }

  var CleanUpCache: Boolean = true

  def cleanUpIfNotCached(
    veColBatch: VeColBatch
  )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): Unit =
    if (cachedBatches.contains(veColBatch))
      logger.debug(s"Data at ${veColBatch.cols
        .map(_.containerLocation)} will not be cleaned up as it's cached (${fullName.value}#${line.value})")
    else
      veColBatch.cols.filterNot(cachedCols.contains).foreach(freeCol)

  val metrics = new ProcessExecutorMetrics(AllocationTracker.simple())

}

class SparkCycloneExecutorPlugin extends ExecutorPlugin with Logging with LazyLogging {
  import com.nec.spark.SparkCycloneExecutorPlugin._veo_proc
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {

    SparkEnv.get.metricsSystem.registerSource(SparkCycloneExecutorPlugin.metrics)
    val resources = ctx.resources()

    logger.info(s"Executor has the following resources available => ${resources}")
    val selectedVeNodeId = if (!resources.containsKey("ve")) {
      val nodeId = Try(System.getenv("VE_NODE_NUMBER").toInt).getOrElse(DefaultVeNodeId)
      logger.info(
        s"Do not have a VE resource available. Will use '${nodeId}' as the main resource."
      )
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

    logger.info(s"Using VE node = ${selectedVeNodeId}")

    if (_veo_proc == null) {
      _veo_proc = veo.veo_proc_create(selectedVeNodeId)
      require(
        _veo_proc != null,
        s"Proc could not be allocated for node ${selectedVeNodeId}, got null"
      )
      require(_veo_proc.address() != 0, s"Address for 0 for proc was ${_veo_proc}")
      logger.info(s"Opened process: ${_veo_proc}")
    }
    logger.info("Initializing SparkCycloneExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
  }

  override def shutdown(): Unit = {
    if (SparkCycloneExecutorPlugin.CleanUpCache) {
      SparkCycloneExecutorPlugin.cleanCache()
    }

    import com.nec.spark.SparkCycloneExecutorPlugin.metrics
    import com.nec.spark.SparkCycloneExecutorPlugin.CloseAutomatically
    import com.nec.spark.SparkCycloneExecutorPlugin.closeProcAndCtx
    Option(metrics.getAllocations)
      .filter(_.nonEmpty)
      .foreach(unfinishedAllocations =>
        logger.error(
          s"There were some ${unfinishedAllocations.size} unreleased allocations: ${unfinishedAllocations.toString
            .take(50)}..., expected to be none."
        )
      )

    val NumToPrint = 5
    Option(metrics.allocationTracker)
      .map(_.remaining)
      .filter(_.nonEmpty)
      .map(_.take(NumToPrint))
      .foreach { someAllocations =>
        logger.error(s"There were some unreleased allocations. First ${NumToPrint}:")
        someAllocations.foreach { allocation =>
          val throwable = new Throwable {
            override def getMessage: String =
              s"Unreleased allocation found at ${allocation.position}"
            override def getStackTrace: Array[StackTraceElement] = allocation.stackTrace.toArray
          }
          logger.error(s"Position: ${allocation.position}", throwable)
        }
      }

    if (CloseAutomatically) {
      logger.info(s"Closing process: ${_veo_proc}")
      closeProcAndCtx()
      launched = false
    }
    super.shutdown()
  }
}
