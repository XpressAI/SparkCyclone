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

import com.nec.native.NativeCompiler
import com.nec.native.NativeCompiler.CachingNativeCompiler
import com.nec.spark.SparkCycloneDriverPlugin.currentCompiler

import scala.collection.JavaConverters.mapAsJavaMapConverter
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.DriverPlugin
import org.apache.spark.api.plugin.PluginContext

import java.nio.file.Files
import com.nec.ve.VeKernelCompiler
import com.typesafe.scalalogging.LazyLogging
import okio.ByteString

object SparkCycloneDriverPlugin {
  // For assumption testing purposes only for now
  private[spark] var launched: Boolean = false
  private[spark] var currentCompiler: NativeCompiler = _
}

class SparkCycloneDriverPlugin extends DriverPlugin with LazyLogging {

  private[spark] var nativeCompiler: NativeCompiler = _
  override def receive(message: Any): AnyRef = {
    message match {
      case RequestCompiledLibraryForCode(code) =>
        logger.debug(s"Received request for compiled code: '${code}'")
        val localLocation = nativeCompiler.forCode(code)
        logger.info(s"Local compiled location = '${localLocation}'")
        RequestCompiledLibraryResponse(ByteString.of(Files.readAllBytes(localLocation): _*))
      case other => super.receive(message)
    }
  }

  override def init(
    sc: SparkContext,
    pluginContext: PluginContext
  ): java.util.Map[String, String] = {
    nativeCompiler = CachingNativeCompiler(NativeCompiler.fromConfig(sc.getConf))
    currentCompiler = nativeCompiler
    logger.info(s"SparkCycloneDriverPlugin is launched. Will use compiler: ${nativeCompiler}")
    logger.info(s"Will use native compiler: ${nativeCompiler}")
    SparkCycloneDriverPlugin.launched = true
    val allExtensions = List(classOf[LocalVeoExtension])
    pluginContext
      .conf()
      .set(
        "spark.sql.extensions",
        allExtensions.map(_.getCanonicalName).mkString(",") + "," + sc.getConf
          .get("spark.sql.extensions", "")
      )

    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
    val testArgs: Map[String, String] = Map(
      "ve_so_name" -> VeKernelCompiler
        .compile_c(
          buildDir = tmpBuildDir,
          config = VeKernelCompiler.VeCompilerConfig.fromSparkConf(pluginContext.conf())
        )
        .toAbsolutePath
        .toString
    )
    testArgs.asJava
  }

  override def shutdown(): Unit = {
    SparkCycloneDriverPlugin.launched = false
  }
}
