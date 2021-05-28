package com.nec

import java.nio.file._
import org.apache.spark.SparkConf

object VeCompiler {
  import VeCompilerConfig.ExtraArgumentPrefix
  final case class VeCompilerConfig(
    optimizationLevel: Int = 4,
    doDebug: Boolean = false,
    additionalOptions: Map[Int, String] = Map.empty,
    useOpenmp: Boolean = true
  ) {
    def compilerArguments: List[String] =
      List(
        s"-O$optimizationLevel",
        "-fpic",
        "-finline-functions",
        "-pthread",
        "-report-all",
        "-fdiag-vector=2"
      ) ++
        List(
          if (doDebug) List("-D", "DEBUG=1") else Nil,
          if (useOpenmp) List("-fopenmp", "-mparallel") else Nil
        ).flatten ++ additionalOptions.toList.sortBy(_._1).map(_._2)

    def include(key: String, value: String): VeCompilerConfig = key match {
      case "o"      => copy(optimizationLevel = value.toInt)
      case "debug"  => copy(doDebug = Set("true", "1").contains(value.toLowerCase))
      case "openmp" => copy(useOpenmp = Set("true", "1").contains(value.toLowerCase))
      case key if key.startsWith(ExtraArgumentPrefix) =>
        copy(additionalOptions =
          additionalOptions.updated(key.drop(ExtraArgumentPrefix.length).toInt, value)
        )
      case other => throw new MatchError(s"Unexpected key for NCC configuration: '${key}'")
    }
  }
  object VeCompilerConfig {
    val ExtraArgumentPrefix = "extra-argument."
    val testConfig: VeCompilerConfig = VeCompilerConfig()
    val NecPrefix = "spark.com.nec.spark.ncc."
    def fromSparkConf(sparkConfig: SparkConf): VeCompilerConfig = {
      sparkConfig
        .getAllWithPrefix(NecPrefix)
        .foldLeft(testConfig) { case (compilerConfig, (key, value)) =>
          compilerConfig.include(key, value)
        }
    }
  }
}
final case class VeCompiler(
  compilationPrefix: String,
  buildDir: Path,
  config: VeCompiler.VeCompilerConfig = VeCompiler.VeCompilerConfig.testConfig
) {
  require(buildDir.toAbsolutePath == buildDir, "Build dir should be absolute")
  def compile_c(sourceCode: String)(nccPath: String = "ncc"): Path = {
    if (!Files.exists(buildDir)) Files.createDirectories(buildDir)
    val cSource = buildDir.resolve(s"${compilationPrefix}.c")

    Files.write(cSource, sourceCode.getBytes())
    val oFile = buildDir.resolve(s"${compilationPrefix}.o")
    val soFile = buildDir.resolve(s"${compilationPrefix}.so")
    import scala.sys.process._
    import config._
    val command: Seq[String] =
      Seq(nccPath) ++ compilerArguments ++ Seq("-c", cSource.toString, "-o", oFile.toString)
    System.err.println(s"Will execute: ${command.mkString(" ")}")
    Process(command = command, cwd = buildDir.toFile).!!

    val command2 = Seq(nccPath, "-shared", "-pthread", "-o", soFile.toString, oFile.toString)
    System.err.println(s"Will execute: ${command2.mkString(" ")}")
    Process(command = command2, cwd = buildDir.toFile).!!

    soFile
  }
}
