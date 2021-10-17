package com.nec.ve

import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.functions.Join.JoinSourceCode
import com.nec.cmake.CMakeBuilder.BuildArguments
import com.nec.cmake.{CMakeBuilder, UdpDebug}
import com.nec.spark.agile.CppResource.CppResources
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import com.typesafe.scalalogging.LazyLogging

import java.nio.file._
import org.apache.spark.SparkConf

object VeKernelCompiler {

  lazy val DefaultIncludes = {
    Set("cpp", "cpp/frovedis", "cpp/frovedis/dataframe", "")
  }

  import VeCompilerConfig.ExtraArgumentPrefix
  final case class ProfileTarget(host: String, port: Int)

  object ProfileTarget {
    def default: ProfileTarget = ProfileTarget(host = "127.0.0.1", port = 45705)

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
    maybeProfileTarget: Option[ProfileTarget] = Some(ProfileTarget.default),
    additionalOptions: Map[Int, String] = Map.empty,
    useOpenmp: Boolean = false
  ) {
    def definitions: List[String] = {
      List(
        if (doDebug) List("DEBUG=1") else Nil,
        maybeProfileTarget.toList.flatMap(tgt =>
          List(
            s"""${UdpDebug.default.hostName}="${tgt.host}"""",
            s"${UdpDebug.default.port}=${tgt.port}"
          )
        )
      ).flatten
    }
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
      ) ++ additionalOptions.toList.sortBy(_._1).map(_._2)

      ret ++ (if (useOpenmp) List("-fopenmp") else Nil)
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

  def compile_c(buildDir: Path = Paths.get("_ve_build"), config: VeCompilerConfig): Path = {
    VeKernelCompiler(CMakeBuilder(buildDir.toAbsolutePath, debug = false), config)
      .compile_c(
        List(TransferDefinitionsSourceCode, JoinSourceCode)
          .mkString("\n\n\n")
      )
  }

  def compile_cpp(
    buildDir: Path = Paths.get("_ve_build"),
    config: VeCompilerConfig,
    code: String
  ): Path = {
    VeKernelCompiler(CMakeBuilder(buildDir.toAbsolutePath, debug = false), config)
      .compile_c(code)
  }

}

final case class VeKernelCompiler(
  cMake: CMakeBuilder,
  config: VeKernelCompiler.VeCompilerConfig = VeCompilerConfig.testConfig
) extends LazyLogging {

  def compile_c(sourceCode: String): Path =
    cMake.buildC(
      sourceCode,
      Some(
        BuildArguments(
          compiler = Some(config.nccPath),
          cxxFlags = Some(config.compilerArguments),
          definitions = Some(config.definitions)
        )
      )
    )
}
