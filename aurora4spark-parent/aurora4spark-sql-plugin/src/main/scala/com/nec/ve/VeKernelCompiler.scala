package com.nec.ve
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.Avg
import com.nec.arrow.functions.Sum
import com.nec.arrow.functions.WordCount
import com.nec.older.AvgMultipleColumns
import com.nec.older.AvgSimple
import com.nec.older.SumMultipleColumns
import com.nec.older.SumPairwise
import com.nec.older.SumSimple
import com.nec.ve.VeKernelCompiler.VeCompilerConfig

import java.nio.file._
import org.apache.spark.SparkConf

object VeKernelCompiler {
  import VeCompilerConfig.ExtraArgumentPrefix
  final case class VeCompilerConfig(
    nccPath: String = "ncc",
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
      case "path"   => copy(nccPath = value)
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

  def compile_c(buildDir: Path = Paths.get("_ve_build"), config: VeCompilerConfig): Path = {
    VeKernelCompiler(compilationPrefix = "_spark", buildDir.toAbsolutePath, config)
      .compile_c(
        List(
          TransferDefinitions.TransferDefinitionsSourceCode,
          WordCount.WordCountSourceCode,
          Avg.AvgSourceCode,
          Sum.SumSourceCode,
          SumSimple.C_Definition,
          SumPairwise.C_Definition,
          AvgSimple.C_Definition,
          SumMultipleColumns.C_Definition,
          AvgMultipleColumns.C_Definition
        ).mkString("\n\n\n")
      )
  }

}
final case class VeKernelCompiler(
  compilationPrefix: String,
  buildDir: Path,
  config: VeKernelCompiler.VeCompilerConfig = VeCompilerConfig.testConfig
) {
  require(buildDir.toAbsolutePath == buildDir, "Build dir should be absolute")
  def compile_c(sourceCode: String): Path = {
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
