package com.nec.ve
import com.nec.arrow.functions.Avg
import com.nec.arrow.functions.Join.JoinSourceCode
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

  val IncludesKey = "spark.com.nec.spark.ncc.includes"

  lazy val DefaultIncludes = DefaultIncludesList
    .mkString(",")

  lazy val RootPath = {
    val current = Paths.get(".").toAbsolutePath
    if (current.toString.contains("fun-bench"))
      current.getParent
    else current
  }

  lazy val DefaultIncludesList = {
    List(
      "src/main/resources/com/nec/arrow/functions/cpp",
      "src/main/resources/com/nec/arrow/functions/cpp/frovedis",
      "src/main/resources/com/nec/arrow/functions/cpp/frovedis/dataframe",
      "src/main/resources/com/nec/arrow/functions",
      "src/main/resources/com/nec/arrow/"
    ).map(sub => RootPath.resolve(sub).toUri)
      .map(
        _.toString
          .replaceAllLiterally("file:///C:/", "C:/")
          .replaceAllLiterally("file://", "")
      )
  }

  import VeCompilerConfig.ExtraArgumentPrefix
  final case class VeCompilerConfig(
    nccPath: String = "ncc",
    optimizationLevel: Int = 4,
    doDebug: Boolean = false,
    additionalOptions: Map[Int, String] = Map.empty,
    includesFromResources: List[String] = List.empty,
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
      case "o"        => copy(optimizationLevel = value.toInt)
      case "debug"    => copy(doDebug = Set("true", "1").contains(value.toLowerCase))
      case "openmp"   => copy(useOpenmp = Set("true", "1").contains(value.toLowerCase))
      case "path"     => copy(nccPath = value)
      case "includes" => copy(includesFromResources = value.split(",").toList)
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
          "#include \"transfer-definitions.h\"",
          WordCount.WordCountSourceCode,
          Avg.AvgSourceCode,
          Sum.SumSourceCode,
          SumSimple.C_Definition,
          SumPairwise.C_Definition,
          AvgSimple.C_Definition,
          SumMultipleColumns.C_Definition,
          AvgMultipleColumns.C_Definition,
          JoinSourceCode
        ).mkString("\n\n\n"),
        config.includesFromResources
      )
  }

}
final case class VeKernelCompiler(
  compilationPrefix: String,
  buildDir: Path,
  config: VeKernelCompiler.VeCompilerConfig = VeCompilerConfig.testConfig
) {
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
    assert(proc.exitValue() == 0, s"Failed; data was: $res; process was ${process}; $resErr")
  }

  def compile_c(sourceCode: String, includes: List[String] = List.empty): Path = {
    if (!Files.exists(buildDir)) Files.createDirectories(buildDir)
    val cSource = buildDir.resolve(s"${compilationPrefix}.c")

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
          "-c",
          cSource.toString,
          "-o",
          oFile.toString
        )
      runHopeOk(Process(command = command, cwd = buildDir.toFile))

      val command2 =
        Seq(nccPath, "-shared", "-pthread") ++ Seq("-o", soFile.toString, oFile.toString)
      runHopeOk(Process(command = command2, cwd = buildDir.toFile))

      soFile
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Failed to compile: ${e}; source was ${cSource}", e)
    }
  }
}
