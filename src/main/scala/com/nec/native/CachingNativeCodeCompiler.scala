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

import scala.collection.mutable.{Map => MMap}
import scala.util.Try
import java.nio.file.{Files, Path, Paths}
import com.typesafe.scalalogging.LazyLogging

object CachingNativeCodeCompiler {
  final val delimiter = ","
  final val extension = ".cidx"
}

final case class CachingNativeCodeCompiler(underlying: NativeCodeCompiler,
                                           buildcache: MMap[Int, CompiledCodeInfo] = MMap.empty)
                                           extends NativeCodeCompiler with LazyLogging {
  logger.info(s"External provided build cache: ${buildcache}")

  // Load the cache from compilation index files on disk
  buildcache ++= loadOnDiskCache
  logger.info(s"Initialized caching compiler with underlying compiler ${underlying} and cache: ${buildcache}")

  def cwd: Path = {
    underlying.cwd.normalize.toAbsolutePath
  }

  private[native] def writeCacheToDisk(funcs: Seq[NativeFunction],
                                       cache: Map[Int, CompiledCodeInfo]): Unit = {
    val indexGroups = cache.toSeq
      // Group by the same Paths
      .groupBy(_._2.path)
      // Turn into Map[Path, Seq[Int]]
      .mapValues(_.map(_._1))

    indexGroups.foreach { case (path, hashes) =>
      val lines = funcs
        // Filter for functions whose hashId is in the bucket
        .filter(x => hashes.contains(x.hashId))
        // For each entry, create a line
        .map { func => s"${func.hashId}${CachingNativeCodeCompiler.delimiter}${func.name}" }

      // Write the compilation index of the .SO file in a corresponding .so.cidx file
      // The format for each line is: `<Function Hash ID> <delim> <Function Name (for debugging purposes)>
      Files.write(Paths.get(s"${path}${CachingNativeCodeCompiler.extension}"), lines.mkString("\n").getBytes)
    }
  }

  private[native] def loadOnDiskCache: Map[Int, CompiledCodeInfo] = {
    val fcwd = cwd.toFile

    // Look for all index files in the build directory
    val ipaths = if (fcwd.exists && fcwd.isDirectory) {
      fcwd.listFiles.toSeq.filter { f =>
        f.isFile && f.getPath.endsWith(CachingNativeCodeCompiler.extension)
      }.map(_.toPath)

    } else {
      Seq.empty[Path]
    }

    logger.info(s"Found the following compilation indices (with ${CachingNativeCodeCompiler.extension} file extension):\n${ipaths.mkString("\n")}\n")

    // Read through each file and accumulate the cache
    val indices = MMap.empty[Int, CompiledCodeInfo]
    ipaths.foreach { ipath =>
      val sopath = Paths.get(s"${ipath}".replaceAll(s"\\${CachingNativeCodeCompiler.extension}$$", ""))

      if (! (sopath.toFile.exists && sopath.toFile.isFile)) {
        // If no .SO file exists, bail
        logger.warn(s"Corresponding .SO file does not exist for compilation index file: ${ipath}")

      } else {
        // Else, parse the file into the cache
        logger.debug(s"Loading compilation index: ${ipath}")

        Files.lines(ipath).toArray.zipWithIndex.foreach { case (line, i) =>
          logger.info(s"Reading line ${i}: ${line}")
          // Split the line
          val tokens = line.asInstanceOf[String].split(CachingNativeCodeCompiler.delimiter)

          val hash = Try { tokens.head.toInt }.getOrElse(0)
          val name = Try { tokens(1) }.getOrElse("_UNKNOWN_")

          indices += (hash -> CompiledCodeInfo(hash, name, sopath))
        }
      }
    }

    indices.toMap
  }

  def build(functions: Seq[NativeFunction]): Map[Int, CompiledCodeInfo] = {
    // Get library paths for the subset of functions that have been previously compiled and cached
    val cached = functions.map { func => buildcache.get(func.hashId).map(info => (func, info)) }.flatten
    logger.info(s"Returning cached .SO info for the following old functions: ${cached.map(_._1.name).mkString("[ ", ", ", " ]")}")

    // Get the subset of functions that have yet been compiled and cached
    val newfuncs = functions.filterNot { func => buildcache.contains(func.hashId) }

    // If there are new functions, compile them and get back the library path mappings
    val newcache = if (newfuncs.nonEmpty) {
      logger.info(s"Building .SO for the following new functions: ${newfuncs.map(_.name).mkString("[ ", ", ", " ]")}")
      underlying.build(newfuncs)

    } else {
      Map.empty[Int, CompiledCodeInfo]
    }

    // Write indices to disk
    writeCacheToDisk(newfuncs, newcache)

    // Update the build cache with the new mappings
    buildcache ++= newcache

    // Return the cached + new mappings
    cached.map { case (_, info) => (info.hashId, info) }.toMap ++ newcache
  }

  def build(code: String): Path = {
    buildcache.get(code.hashCode) match {
      case Some(info) =>
        logger.debug(s"Cache hit for compilation.")
        info.path

      case None =>
        logger.debug(s"Cache miss for compilation.")
        val path = underlying.build(code)

        // Update the build cache with the new mappings
        buildcache += (code.hashCode -> CompiledCodeInfo(code.hashCode, "_RAW_CODE_", path))
        path
    }
  }
}
