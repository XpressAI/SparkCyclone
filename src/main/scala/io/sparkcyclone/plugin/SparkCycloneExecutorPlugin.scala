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
package io.sparkcyclone.plugin

import io.sparkcyclone.cache.VeColBatchesCache
import io.sparkcyclone.data.vector._
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.metrics.ProcessExecutorMetrics
import io.sparkcyclone.spark.transformation.{RequestLibCyclone, RequestLibCycloneResponse}
import io.sparkcyclone.vectorengine._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.{Map => JMap}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

object SparkCycloneExecutorPlugin {
  var pluginContext: PluginContext = _

  var params = TrieMap.empty[String, String]

  var NodeCount = 1

  // Used for tests
  var CloseAutomatically = true

  def totalVeCores: Int = {
    NodeCount * 8
  }

  @transient implicit var veProcess: VeProcess = DeferredVeProcess { () =>
    require(pluginContext != null, s"${classOf[PluginContext].getSimpleName} has not been set yet!")
    VeProcess.createFromContext(pluginContext)
  }

  implicit def source: VeColVectorSource = {
    veProcess.source
  }

  @transient implicit lazy val veMetrics: ProcessExecutorMetrics = {
    require(pluginContext != null, s"${classOf[PluginContext].getSimpleName} is not yet set!")
    new ProcessExecutorMetrics(pluginContext.metricRegistry)
  }

  @transient implicit lazy val vectorEngine: VectorEngine = {
    require(pluginContext != null, s"${classOf[PluginContext].getSimpleName} is not yet set!")
    new VectorEngineImpl(veProcess, pluginContext.metricRegistry)
  }

  @transient val batchesCache = new VeColBatchesCache
}

class SparkCycloneExecutorPlugin extends ExecutorPlugin with Logging with LazyLogging {
  private[plugin] def laodLibCyclone(context: PluginContext): LibraryReference = {
    logger.info(s"Fetching libcyclone.so from ${classOf[SparkCycloneDriverPlugin].getSimpleName}...")

    // Fetch libcyclone.so from the driver plugin and write to disk
    val libpath = context.ask(RequestLibCyclone) match {
      case RequestLibCycloneResponse(dpath, bytes) =>
        val destpath = Paths.get(dpath)
        if (! Files.exists(destpath)) {
          Files.write(destpath, bytes.toArray, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        } else {
          destpath
        }
    }

    // Load libcyclone.so
    SparkCycloneExecutorPlugin.veProcess.load(libpath)
  }

  override def init(context: PluginContext, conf: JMap[String, String]): Unit = {
    logger.info(s"Initializing ${getClass.getSimpleName}...")

    // Set the plugin context
    SparkCycloneExecutorPlugin.pluginContext = context

    // Update the plugin params
    SparkCycloneExecutorPlugin.params ++= conf.asScala

    // Update the node count as needed
    SparkCycloneExecutorPlugin.NodeCount = {
      val resources = context.resources
      if (resources.containsKey("ve")) resources.get("ve").addresses.length else 1
    }

    // Start the actual VE process by calling a method that will initialize it
    SparkCycloneExecutorPlugin.veProcess.apiVersion

    /*
      Fetch and load libcyclone.so from the driver plugin, so that Cyclone C++
      library functions such as `cyclone_handle_transfer` can be called without
      the need to load a pre-built library.
    */
    laodLibCyclone(context)
    logger.info(s"Loaded libcyclone.so as part of ${getClass.getSimpleName} initialization...")
  }

  override def shutdown: Unit = {
    import SparkCycloneExecutorPlugin.veProcess
    logger.info(s"Shutting down ${getClass.getSimpleName}...")

    logger.info(s"Clearing the VeColBatch cache...")
    SparkCycloneExecutorPlugin.batchesCache.cleanup

    if (SparkCycloneExecutorPlugin.CloseAutomatically) {
      logger.info(s"Shutting down the VE process...")
      SparkCycloneExecutorPlugin.veProcess.close
    }

    super.shutdown
  }
}
