package com.nec.ve

import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.functions.Join.JoinSourceCode
import com.nec.cmake.UdpDebug
import com.nec.spark.agile.CppResource.CppResources
import com.nec.ve.VeKernelCompiler.{DefaultIncludes, VeCompilerConfig}
import com.typesafe.scalalogging.LazyLogging

import java.nio.file._
import org.apache.spark.SparkConf

object VeKernelCompiler {

  lazy val DefaultIncludes = Set(
    "sources/cpp",
    "/opt/nec/frovedis/ve/include",
    "/opt/nec/frovedis/ve/include/frovedis",
    "/opt/nec/frovedis/ve/include/frovedis/core",
    "/opt/nec/frovedis/ve/include/frovedis/text",
    "/opt/nec/ve/mpi/2.18.0/include/",
    "/opt/nec/frovedis/ve/opt/boost/include",
    "."
  )

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
              s"""${UdpDebug.default.hostName}="${tgt.host}"""",
              "-D",
              s"${UdpDebug.default.port}=${tgt.port}"
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

  def compile_c(buildDir: Path = Paths.get("_ve_build"), config: VeCompilerConfig): Path = {
    VeKernelCompiler(compilationPrefix = "_spark", buildDir.toAbsolutePath, config)
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
    VeKernelCompiler(compilationPrefix = "_spark", buildDir.toAbsolutePath, config)
      .compile_c(code)
  }

}

final case class VeKernelCompiler(
  compilationPrefix: String,
  buildDir: Path,
  config: VeKernelCompiler.VeCompilerConfig = VeCompilerConfig.testConfig
) extends LazyLogging {
  require(buildDir.toAbsolutePath == buildDir, "Build dir should be absolute")

  import scala.sys.process._

  def runHopeOk(process: ProcessBuilder): Unit = {
    var res = ""
    var resErr = ""
    val io = new ProcessIO(
      stdin => { stdin.close() },
      stdout => {
        val src = scala.io.Source.fromInputStream(stdout)
        try res = src.mkString
        finally stdout.close()
      },
      stderr => {
        val src = scala.io.Source.fromInputStream(stderr)
        try resErr = src.mkString
        finally stderr.close()
      }
    )
    val proc = process.run(io)
    val ev = proc.exitValue()
    if (config.doDebug) {
      logger.debug(s"NCC output: \n${res}; \n${resErr}")
    }
    assert(ev == 0, s"Failed; data was: $res; process was ${process}; $resErr")
  }

  def compile_c(sourceCode: String): Path = {
    if (!Files.exists(buildDir)) Files.createDirectories(buildDir)
    val cSource = buildDir.resolve(s"${compilationPrefix}.c")

    val sourcesDir = buildDir.resolve("sources")
    CppResources.All.copyTo(sourcesDir)

    Files.write(cSource, sourceCode.getBytes())
    try {
      val oFile = buildDir.resolve(s"${compilationPrefix}.o")
      val soFile = buildDir.resolve(s"${compilationPrefix}.so")
      import scala.sys.process._
      import config._
      val includesArgs = DefaultIncludes.map(i => s"-I${i}")
      val command: Seq[String] =
        Seq(nccPath) ++ compilerArguments ++ includesArgs ++ Seq(
          "-xc++",
          "-c",
          cSource.toString,
          "-o",
          oFile.toString
        )
      runHopeOk(Process(command = command, cwd = buildDir.toFile))

      val command2 =
        Seq(nccPath, "-shared", "-pthread" /*, "-ftrace", "-lveftrace_p"*/ ) ++
          Seq(
            "-o",
            soFile.toString,
            oFile.toString,
            "-L/opt/nec/frovedis/ve/lib",
            "-lfrovedis_core",
            "-lfrovedis_text",
          )

      runHopeOk(Process(command = command2, cwd = buildDir.toFile))

      soFile
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Failed to compile: ${e}; source was ${cSource}", e)
    }
  }
}
