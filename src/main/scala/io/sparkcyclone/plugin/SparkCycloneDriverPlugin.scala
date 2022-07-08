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

import io.sparkcyclone.native.{CachingNativeCodeCompiler, NativeCodeCompiler}
import io.sparkcyclone.spark.{RequestCompiledLibraryForCode, RequestCompiledLibraryResponse}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.util.{Map => JMap}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import okio.ByteString

object SparkCycloneDriverPlugin {
  // For assumption testing purposes only for now
  var launched: Boolean = false

  @transient var currentCompiler: NativeCodeCompiler = _
}

class SparkCycloneDriverPlugin extends DriverPlugin with LazyLogging {
  override def init(sparkContext: SparkContext,
                    pluginContext: PluginContext): JMap[String, String] = {
    logger.info(s"Initializing ${getClass.getSimpleName}...")

    // Initialize the native code compiler
    SparkCycloneDriverPlugin.currentCompiler = NativeCodeCompiler.createFromContext(sparkContext.getConf, pluginContext)
    logger.info(s"Using native code compiler: ${SparkCycloneDriverPlugin.currentCompiler}")

    // Inject the extensions into the Spark plugin context
    val extensions = Seq(classOf[LocalVeoExtension])
    pluginContext.conf.set(
      "spark.sql.extensions",
      extensions.map(_.getCanonicalName).mkString(",") + "," + sparkContext.getConf.get("spark.sql.extensions", "")
    )
    logger.info(s"Injected Spark SQL extensions ${extensions} into the Spark plugin context")

    SparkCycloneDriverPlugin.launched = true
    Map.empty[String, String].asJava
  }

  override def receive(message: Any): AnyRef = {
    message match {
      case RequestCompiledLibraryForCode(path) =>
        logger.debug(s"Received request for compiled code at path: '${path}'")
        val location = Paths.get(path)

        if (Files.exists(location)) {
          RequestCompiledLibraryResponse(ByteString.of(Files.readAllBytes(location): _*))
        } else {
          throw new RuntimeException(s"Received request for code at path ${path} but it's not present on driver.")
        }
    }
  }

  override def shutdown: Unit = {
    logger.info(s"Shutting down ${getClass.getSimpleName}...")
    SparkCycloneDriverPlugin.launched = false
  }
}
