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
package com.nec.ve

import com.nec.cmake.TcpDebug
import com.nec.spark.agile.CppResource.CppResources
import com.nec.ve.VeKernelCompiler.{FileAttributes, PlatformLibrarySoName, VeCompilerConfig}
import com.typesafe.scalalogging.LazyLogging

import java.nio.file._
import org.apache.spark.SparkConf

import java.nio.file.attribute.PosixFilePermission.{
  GROUP_EXECUTE,
  GROUP_READ,
  OTHERS_EXECUTE,
  OTHERS_READ,
  OWNER_EXECUTE,
  OWNER_READ,
  OWNER_WRITE
}
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.util

object VeKernelCompiler {
  import scala.collection.JavaConverters._

  val PosixPermissions: util.Set[PosixFilePermission] = Set[PosixFilePermission](
    OWNER_READ,
    OWNER_WRITE,
    OWNER_EXECUTE,
    GROUP_READ,
    GROUP_EXECUTE,
    OTHERS_READ,
    OTHERS_EXECUTE
  ).asJava
  val FileAttributes = PosixFilePermissions.asFileAttribute(PosixPermissions)

  import VeCompilerConfig.ExtraArgumentPrefix
  final case class ProfileTarget(host: String, port: Int)

  object ProfileTarget {
    def parse(value: String): Option[ProfileTarget] = {
      PartialFunction.condOpt(value.split(':')) { case Array(h, p) =>
        ProfileTarget(h, p.toInt)
      }
    }
  }

  final case class VeCompilerConfig(
    nccPath: String = "/opt/nec/ve/bin/ncc",
    optimizationLevel: Int = 4,
    doDebug: Boolean = false,
    maybeProfileTarget: Option[ProfileTarget] = None,
    additionalOptions: Map[Int, String] = Map.empty,
    useOpenmp: Boolean = false
  ) {
    def compilerArguments: List[String] = {
      // Optimizations used in frovedis: -fno-defer-inline-template-instantiation -finline-functions -finline-max-depth = 10 -msched-block
      val ret = List(
        s"-O$optimizationLevel",
        "-fpic",
        "-fno-defer-inline-template-instantiation",
        "-finline-functions",
        "-finline-max-depth=10",
        "-msched-block",
        "-pthread",
        "-report-all",
        /* "-ftrace", */
        "-fdiag-vector=2"
      ) ++
        List(
          if (doDebug) List("-D", "DEBUG=1") else Nil,
          if (useOpenmp) List("-fopenmp") else Nil,
          maybeProfileTarget.toList.flatMap(tgt =>
            List(
              "-D",
              s"""${TcpDebug.default.hostName}="${tgt.host}"""",
              "-D",
              s"${TcpDebug.default.port}=${tgt.port}"
            )
          )
        ).flatten ++ additionalOptions.toList.sortBy(_._1).map(_._2)
      ret
    }

    def include(key: String, value: String): VeCompilerConfig = key match {
      case "o"              => copy(optimizationLevel = value.toInt)
      case "profile-target" => copy(maybeProfileTarget = ProfileTarget.parse(value))
      case "debug"          => copy(doDebug = Set("true", "1").contains(value.toLowerCase))
      case "openmp"         => copy(useOpenmp = Set("true", "1").contains(value.toLowerCase))
      case "path"           => copy(nccPath = value)
      case key if key.startsWith(ExtraArgumentPrefix) =>
        copy(additionalOptions =
          additionalOptions.updated(key.drop(ExtraArgumentPrefix.length).toInt, value)
        )
      case other => throw new MatchError(s"Unexpected key for NCC configuration: '${key}'")
    }
  }
  object VeCompilerConfig {
    val ExtraArgumentPrefix = "extra-argument."
    val NecNonSparkPrefix = "ncc."
    val testConfig: VeCompilerConfig = {
      import scala.collection.JavaConverters._
      System.getProperties.asScala
        .collect {
          case (k, v) if k.startsWith(NecNonSparkPrefix) && v != null =>
            k.drop(NecNonSparkPrefix.length) -> v
        }
        .foldLeft(VeCompilerConfig()) { case (compilerConfig, (key, value)) =>
          compilerConfig.include(key, value)
        }
    }
    val NecPrefix = "spark.com.nec.spark.ncc."
    def fromSparkConf(sparkConfig: SparkConf): VeCompilerConfig = {
      sparkConfig
        .getAllWithPrefix(NecPrefix)
        .foldLeft(testConfig) { case (compilerConfig, (key, value)) =>
          compilerConfig.include(key, value)
        }
    }
  }

  def compile_cpp(
    buildDir: Path = Paths.get("_ve_build"),
    config: VeCompilerConfig,
    code: String
  ): Path = {
    VeKernelCompiler(compilationPrefix = "_spark", buildDir.toAbsolutePath, config)
      .compile_c(code)
  }

  val PlatformLibrarySoName = "libcyclone.so"

}

final case class VeKernelCompiler(
  compilationPrefix: String,
  buildDir: Path,
  config: VeKernelCompiler.VeCompilerConfig = VeCompilerConfig.testConfig
) extends LazyLogging {
  require(buildDir.toAbsolutePath == buildDir, "Build dir should be absolute")

  def compile_c(sourceCode: String): Path = {
    if (!Files.exists(buildDir)) {

      Files.createDirectories(buildDir, FileAttributes)
    }
    val cSource = buildDir.resolve(s"${compilationPrefix}.c")

    val sourcesDir = buildDir.resolve("sources")
    CppResources.AllVe.copyTo(sourcesDir)
    val includes: List[String] = {
      CppResources.AllVe.all
        .map(_.containingDir(sourcesDir))
        .toList
        .map(i => i.toUri.toString.drop(sourcesDir.getParent.toUri.toString.length))
    }
    val linkSos =
      CppResources.AllVe.all.filter(_.name.endsWith(".so"))

    assert(
      linkSos.nonEmpty,
      s"Expected to have at least 1 .so file, found nond. Source: ${CppResources.AllVe}"
    )
    Files.write(cSource, sourceCode.getBytes())
    try {
      val oFile = buildDir.resolve(s"${compilationPrefix}.o")
      val soFile = buildDir.resolve(s"${compilationPrefix}.so")
      import scala.sys.process._
      import config._
      val includesArgs = includes.map(i => s"-I${i}")
      val command: Seq[String] =
        Seq(nccPath) ++ compilerArguments ++ includesArgs ++ Seq(
          "-xc++",
          "-Isources/",
          "-c",
          cSource.toString,
          "-o",
          oFile.toString
        )
      logger.info(s"Compilation command = ${command}")

      ProcessRunner.runHopeOk(
        Process(command = command, cwd = buildDir.toFile),
        doDebug = config.doDebug
      )
      // make sure everyone can read this
      ProcessRunner.runHopeOk(
        Process(command = List("chmod", "777", oFile.toString), cwd = buildDir.toFile),
        doDebug = config.doDebug
      )

      val command2: Seq[String] = {
        Seq(nccPath, "-shared", "-pthread" /*, "-ftrace", "-lveftrace_p"*/ ) ++ Seq(
          "-o",
          soFile.toString,
          oFile.toString
        ) ++ linkSos.toList.map(_.name).map(sourcesDir.resolve(_)).map(_.toString)
      }
      ProcessRunner.runHopeOk(
        Process(command = command2, cwd = buildDir.toFile),
        doDebug = config.doDebug
      )

      soFile
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Failed to compile: ${e}; source was in: ${cSource}", e)
    }
  }
}
