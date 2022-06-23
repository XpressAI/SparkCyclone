/*
 * Copyright (c) 2022 Xpress AI.
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
package com.nec.native

import com.nec.native.compiler._
import com.nec.spark.agile.core.{CFunction2, CodeLines}
import java.nio.file.{Files, Path, Paths}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext

trait NativeFunction {
  final def name: String = {
    func.name
  }

  /*
    The hashId of the function should be a hash of the "semantic identity" of
    the function, rather than that of the concrete function body itself.
  */
  def hashId: Int

  def func: CFunction2
}

trait NativeCodeCompiler extends Serializable {
  private[native] def combinedCode(functions: Seq[NativeFunction]): String = {
    val cfunctions = functions.map(_.func)
    val headers = cfunctions.map(_.additionalHeaders).flatten.toSet

    CodeLines.from(
      headers.map(_.toString).toSeq,
      "",
      cfunctions.map(_.toCodeLines)
    ).cCode
  }

  def build(functions: Seq[NativeFunction]): Map[Int, Path]

  def build(code: String): Path
}

object NativeCodeCompiler extends LazyLogging {
  def createFromContext(config: SparkConf,
                        context: PluginContext): NativeCodeCompiler = {
    val veconfig = VeCompilerConfig.fromSparkConf(config)

    // Allow for possible other underlying compilers in the future
    val underlying = config.getOption("spark.com.nec.spark.kernel.directory").map(Paths.get(_))  match {
      case Some(dir) =>
        OnDemandVeCodeCompiler(dir, veconfig)

      case None =>
        val dir = Files.createTempDirectory("ve-spark-tmp", VeKernelCompilation.FileAttributes).toAbsolutePath
        OnDemandVeCodeCompiler(dir, veconfig)
    }

    // Extend compiler with a cache
    CachingNativeCodeCompiler(underlying)
  }
}
