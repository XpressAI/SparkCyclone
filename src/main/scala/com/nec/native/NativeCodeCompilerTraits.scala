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
import scala.collection.concurrent.{TrieMap => MMap}
import java.nio.file.{Files, Path, Paths}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf

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
  def fromSparkConfig(config: SparkConf): NativeCodeCompiler = {
    val veconfig = VeCompilerConfig.fromSparkConf(config)

    // Allow for possible other underlying compilers in the future
    val underlying = config.getOption("spark.com.nec.spark.kernel.directory").map(Paths.get(_))  match {
      case Some(dir) =>
        OnDemandVeCodeCompiler(dir, veconfig)

      case None =>
        val dir = Files.createTempDirectory("ve-spark-tmp", VeKernelCompilation.FileAttributes).toAbsolutePath
        OnDemandVeCodeCompiler(dir, veconfig)
    }

    CachingNativeCodeCompiler(underlying)
  }
}

final case class CachingNativeCodeCompiler(underlying: NativeCodeCompiler,
                                           buildcache: MMap[Int, Path] = MMap.empty)
                                           extends NativeCodeCompiler with LazyLogging {
  logger.info(s"Initialized caching compiler with underlying compiler ${underlying} and cache: ${buildcache}")

  def build(functions: Seq[NativeFunction]): Map[Int, Path] = {
    // Get library paths for the subset of functions that have been previously compiled and cached
    val cached = functions.map { func => buildcache.get(func.hashId).map(x => (func, x)) }.flatten
    logger.info(s"Returning cached .SO paths for the following old functions: ${cached.map(_._1.name)}")

    // Get the subset of functions that have yet been compiled and cached
    val newfuncs = functions.filterNot { func => buildcache.contains(func.hashId) }

    // If there are new functions, compile them and get back the library path mappings
    val newcache = if (newfuncs.nonEmpty) {
      logger.info(s"Building .SO for the following new functions: ${newfuncs.map(_.name)}")
      underlying.build(newfuncs)

    } else {
      Map.empty[Int, Path]
    }

    // Update the build cache with the new mappings
    buildcache ++= newcache

    // Return the cached + new mappings
    cached.map { case (func, path) => (func.hashId, path) }.toMap ++ newcache
  }

  def build(code: String): Path = {
    buildcache.get(code.hashCode) match {
      case Some(path) =>
        logger.debug(s"Cache hit for compilation.")
        path

      case None =>
        logger.debug(s"Cache miss for compilation.")
        val path = underlying.build(code)
        buildcache ++= Map(code.hashCode -> path)
        path
    }
  }
}
