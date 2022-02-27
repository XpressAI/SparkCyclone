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
package com.nec.native
import com.typesafe.scalalogging.LazyLogging

import java.nio.file.Paths
import com.nec.cmake.CMakeBuilder
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.apache.spark.SparkConf

import java.nio.file.Files
import java.nio.file.Path
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.{FileAttributes, VeCompilerConfig}

trait NativeCompiler extends Serializable {

  /** Location of the compiled kernel library */
  def forCode(code: String): Path
  def forCode(codeLines: CodeLines): Path = forCode(codeLines.cCode)
  protected def combinedCode(code: String): String =
    List(code).mkString("\n\n")
}

object NativeCompiler extends LazyLogging {
  def fromConfig(sparkConf: SparkConf): NativeCompiler = {
    val compilerConfig = VeKernelCompiler.VeCompilerConfig.fromSparkConf(sparkConf)
    logger.info("Compiler configuration: {}", compilerConfig)
    sparkConf.getOption("spark.com.nec.spark.kernel.precompiled") match {
      case Some(directory) => PreCompiled(directory)
      case None =>
        sparkConf.getOption("spark.com.nec.spark.kernel.directory") match {
          case Some(directory) =>
            OnDemandCompilation(directory, compilerConfig)
          case None =>
            fromTemporaryDirectory(compilerConfig)._2
        }
    }
  }

  def fromTemporaryDirectory(compilerConfig: VeCompilerConfig): (Path, NativeCompiler) = {
    val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp", FileAttributes)
    (tmpBuildDir, OnDemandCompilation(tmpBuildDir.toAbsolutePath.toString, compilerConfig))
  }

  final case class CachingNativeCompiler(
    nativeCompiler: NativeCompiler,
    var cache: Map[Int, Path] = Map.empty
  ) extends NativeCompiler
    with LazyLogging {

    def getCached(codeHash: Int): Option[Path] = {
      cache.get(codeHash)
    }

    /** Location of the compiled kernel library */
    override def forCode(code: String): Path = {
      cache.get(code.hashCode) match {
        case None =>
          logger.debug("Cache miss for compilation.")
          val compiledPath = nativeCompiler.forCode(code)
          cache = cache.updated(code.hashCode, compiledPath)
          compiledPath
        case Some(path) =>
          logger.debug("Cache hit for compilation.")
          path
      }
    }
  }

  final case class OnDemandCompilation(buildDir: String, veCompilerConfig: VeCompilerConfig)
    extends NativeCompiler
    with LazyLogging {
    override def forCode(code: String): Path = {
      val cc = combinedCode(code)
      val sourcePath = Paths.get(buildDir).resolve(s"_spark_${cc.hashCode}.so").toAbsolutePath

      if (sourcePath.toFile.exists()) {
        logger.debug("Loading precompiled from path: {}", sourcePath)
        sourcePath
      } else {
        logger.debug("Compiling for the VE...:{}", code)
        logger.info("Compiler config ==> {}", veCompilerConfig)
        val startTime = System.currentTimeMillis()
        val soName =
          VeKernelCompiler(
            compilationPrefix = s"_spark_${cc.hashCode}",
            Paths.get(buildDir),
            veCompilerConfig
          )
            .compile_c(cc)
        val endTime = System.currentTimeMillis() - startTime
        logger.debug("Compiled code in {}ms to path {}.", endTime, soName)
        soName
      }
    }
  }

  final case class PreCompiled(sourceDir: String) extends NativeCompiler with LazyLogging {
    override def forCode(code: String): Path = {
      val cc = combinedCode(code)
      val sourcePath = Paths.get(sourceDir).resolve(s"_spark_${cc.hashCode}.so").toAbsolutePath
      logger.debug("Will be loading source from path: {}", sourcePath)
      sourcePath
    }
  }

  object CNativeCompiler extends NativeCompiler {
    override def forCode(code: String): Path = {
      CMakeBuilder.buildCLogging(
        List(code)
          .mkString("\n\n")
      )
    }
  }

  object CNativeCompilerDebug extends NativeCompiler {
    override def forCode(code: String): Path = {
      CMakeBuilder.buildCLogging(
        cSource = List(code)
          .mkString("\n\n"),
        debug = true
      )
    }
  }

}
