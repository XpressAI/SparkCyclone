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
import com.typesafe.scalalogging.LazyLogging
import okio.ByteString
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters.mapAsJavaMapConverter

object SparkCycloneDriverPlugin {
  // For assumption testing purposes only for now
  private[spark] var launched: Boolean = false
  private[spark] var currentCompiler: NativeCompiler = _
}

class SparkCycloneDriverPlugin extends DriverPlugin with LazyLogging {

  private[spark] var nativeCompiler: NativeCompiler = _
  override def receive(message: Any): AnyRef = {
    message match {
      case RequestCompiledLibraryForCode(codePath) =>
        logger.debug(s"Received request for compiled code at path: '${codePath}'")
        val localLocation = Paths.get(codePath)
        if (Files.exists(localLocation)) {
          RequestCompiledLibraryResponse(ByteString.of(Files.readAllBytes(localLocation): _*))

        } else {
          throw new RuntimeException(s"Received request for code at path ${codePath} but it's not present on driver.")
        }
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

    val testArgs: Map[String, String] = Map.empty
    testArgs.asJava
  }

  override def shutdown(): Unit = {
    SparkCycloneDriverPlugin.launched = false
  }
}
