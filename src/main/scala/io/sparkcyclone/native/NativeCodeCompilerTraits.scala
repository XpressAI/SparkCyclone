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
package io.sparkcyclone.native

import io.sparkcyclone.native.compiler._
import io.sparkcyclone.native.code.{CFunction2, CodeLines}
import java.nio.file.{Files, Path, Paths}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext

trait NativeFunction {
  /*
    The name of the primary function, which will be the symbol available for the
    VE process to invoke upon.
  */
  final def identifier: String = {
    primary.name
  }

  /*
    All the CFunctions that are packaged in this NativeFunction definition.
  */
  final def cfunctions: Seq[CFunction2] = {
    Seq(primary) ++ secondary
  }

  /*
    Render as CodeLines (used for tests)
  */
  final def codelines: CodeLines = {
    val headers = cfunctions.map(_.additionalHeaders).flatten.toSet ++ CFunction2.DefaultHeaders
    CodeLines.from(
      headers.toSeq.sortBy(_.name).map(_.toString),
      "",
      cfunctions.map(_.declaration),
      cfunctions.map(_.definition)
    )
  }

  /*
    The hashId of the function group should be a hash of the "semantic identity"
    of the function, rather than that of the concrete function body itself.
  */
  def hashId: Int

  /*
    This is the function whose compiled symbol will be invoked by the VE process
    as part of the execution of a Spark Plan.
  */
  def primary: CFunction2

  /*
    A list of secondary functions that are intended to be called from the body
    of the primary function, but NOT intended to be called directly by the VE
    process itself.  This abstraction is provided to support native code generated
    for advanced SQL operations that are defined in groups of functions, such
    as joins.
  */
  def secondary: Seq[CFunction2]
}

case class CompiledCodeInfo(hashId: Int,
                            name: String,
                            path: Path)

trait NativeCodeCompiler extends Serializable {
  private[native] def combinedCode(functions: Seq[NativeFunction]): String = {
    val cfuncs = functions.flatMap(_.cfunctions)
    val headers = cfuncs.map(_.additionalHeaders).flatten.toSet ++ CFunction2.DefaultHeaders

    CodeLines.from(
      headers.toSeq.sortBy(_.name).map(_.toString),
      "",
      // Put all function declarations first before the definitions
      cfuncs.map(_.declaration),
      cfuncs.map(_.definition)
    ).cCode
  }

  def cwd: Path

  def build(functions: Seq[NativeFunction]): Map[Int, CompiledCodeInfo]

  def build(code: String): Path
}

object NativeCodeCompiler extends LazyLogging {
  def createFromContext(config: SparkConf,
                        context: PluginContext): NativeCodeCompiler = {
    val veconfig = VeCompilerConfig.fromSparkConf(config)

    // Allow for possible other underlying compilers in the future
    val underlying = config.getOption("spark.cyclone.kernel.directory").map(Paths.get(_))  match {
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
