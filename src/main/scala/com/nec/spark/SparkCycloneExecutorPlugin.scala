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

import com.nec.spark.SparkCycloneExecutorPlugin.{DefaultVeNodeId, ImplicitMetrics, launched, params, pluginContext}
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.nec.ve.{VeColBatch, VeProcess, VeProcessMetrics}
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
import scala.collection.convert.ImplicitConversions.`collection asJava`
import scala.collection.mutable
import scala.util.Try

object SparkCycloneExecutorPlugin extends LazyLogging {

  /** For assumption testing purposes only for now */
  var params: Map[String, String] = Map.empty[String, String]

  var pluginContext: PluginContext = _

  /** For assumption testing purposes only for now */
  private[spark] var launched: Boolean = false
  var _veo_proc: veo_proc_handle = _
  @transient val libsPerProcess: scala.collection.mutable.Map[
    veo_proc_handle,
    scala.collection.mutable.Map[String, LibraryReference]
  ] =
    scala.collection.mutable.Map.empty

  implicit def veProcess: VeProcess =
    VeProcess.DeferredVeProcess(() =>
      VeProcess.WrappingVeo(_veo_proc, source, ImplicitMetrics.processMetrics)
    )

  implicit def source: VeColVectorSource = VeColVectorSource(
    s"Process ${Option(_veo_proc)}, executor ${Option(SparkEnv.get).flatMap(se => Option(se.executorId))}"
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
  var NodeCount = 1

  def totalVeCores(): Int = {
    NodeCount * 8
  }

  var theMetrics: ProcessExecutorMetrics = _

  object ImplicitMetrics {
    implicit def processMetrics: VeProcessMetrics = theMetrics
  }

  var CleanUpCache: Boolean = true

  @transient private val cachedBatches: mutable.Map[String, VeColBatch] = mutable.HashMap.empty

  @transient private val cachedCols: mutable.Map[String, VeColVector] = mutable.HashMap.empty

  private def cleanCache()(implicit originalCallingContext: OriginalCallingContext): Unit = {
    cachedBatches.toList.foreach { colBatch =>
      cachedBatches.remove(colBatch._1)
      colBatch._2.cols.zipWithIndex.foreach { case (_, i) =>
        freeCachedCol(s"${colBatch._1}-${i}")
      }
    }
  }

  def freeCachedCol(
    col: String
  )(implicit originalCallingContext: OriginalCallingContext): Unit = {
    if (cachedCols.contains(col)) {
      cachedCols(col).free()
      cachedCols.remove(col)
    }
  }

  def containsCachedBatch(name: String): Boolean = cachedBatches.contains(name)
  def getCachedBatch(name: String): VeColBatch = cachedBatches(name)

  def registerCachedBatch(name: String, cb: VeColBatch): Unit = {
    cachedBatches(name) = cb

    cb.cols.zipWithIndex.foreach { case (col, i) =>
      cachedCols(s"$name-$i") = col
    }
  }


  def cleanUpIfNotCached(
    veColBatch: VeColBatch
  )(implicit originalCallingContext: OriginalCallingContext): Unit = {
    if (cachedBatches.values.contains(veColBatch)) {
      logger.trace(
        s"Data at ${
          veColBatch.cols
            .map(_.container)
        } will not be cleaned up as it's cached (${originalCallingContext.fullName.value}#${originalCallingContext.line.value})"
      )
    } else {
      val (cached, notCached) = veColBatch.cols.partition(cachedCols.values.contains)
      logger.trace(s"Will clean up data for ${
        cached
          .map(_.buffers)
      }, and not clean up for ${notCached.map(_.allocations)}")
      notCached.foreach(_.free())
    }
  }
}

class SparkCycloneExecutorPlugin extends ExecutorPlugin with Logging with LazyLogging {
  import com.nec.spark.SparkCycloneExecutorPlugin._veo_proc
  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    SparkCycloneExecutorPlugin.theMetrics =
      new ProcessExecutorMetrics(AllocationTracker.simple(), ctx.metricRegistry())
    //SparkEnv.get.metricsSystem.registerSource(SparkCycloneExecutorPlugin.metrics)

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
      SparkCycloneExecutorPlugin.NodeCount = resourceCount
      val executorNumber = Try(ctx.executorID().toInt - 1).getOrElse(0) // Executor IDs start at 1.
      val veMultiple = executorNumber / 8
      if (veMultiple > resourceCount) {
        logWarning("Not enough VE resources allocated for number of executors specified.")
      }
      veResources.addresses(veMultiple % resourceCount).toInt
    }

    logger.info(s"Using VE node = ${selectedVeNodeId}")

    var currentVeNodeId = selectedVeNodeId
    while (_veo_proc == null && currentVeNodeId < 8) {
      _veo_proc = veo.veo_proc_create(currentVeNodeId)
      if (_veo_proc == null) {
        logWarning(s"Proc could not be allocated for node ${currentVeNodeId}, trying next.")
        currentVeNodeId += 1
      }
    }
    require(
      _veo_proc != null,
      s"Proc could not be allocated for node ${selectedVeNodeId}, got null"
    )
    require(_veo_proc.address() > 0, s"Address for proc was ${_veo_proc.address()}")
    logger.info(s"Opened process: ${_veo_proc} Address: ${_veo_proc.address()}")

    logger.info("Initializing SparkCycloneExecutorPlugin.")
    params = params ++ extraConf.asScala
    launched = true
    pluginContext = ctx
  }

  override def shutdown(): Unit = {
    if (SparkCycloneExecutorPlugin.CleanUpCache) {
      import OriginalCallingContext.Automatic._

      SparkCycloneExecutorPlugin.cleanCache()
    }

    import com.nec.spark.SparkCycloneExecutorPlugin.{CloseAutomatically, closeProcAndCtx}
    Option(ImplicitMetrics.processMetrics.getAllocations)
      .filter(_.nonEmpty)
      .foreach(unfinishedAllocations =>
        logger.error(
          s"There were some ${unfinishedAllocations.size} unreleased allocations: ${unfinishedAllocations.toString
            .take(50)}..., expected to be none."
        )
      )

    val NumToPrint = 5
    Option(ImplicitMetrics.processMetrics.allocationTracker)
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
