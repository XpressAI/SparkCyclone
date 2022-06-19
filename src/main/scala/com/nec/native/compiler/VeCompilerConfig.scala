package com.nec.native.compiler

import scala.collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf

object VeCompilerConfig {
  object Defaults {
    final val NccPath = "/opt/nec/ve/bin/nc++"
    final val OptLevel = 4
    final val DoDebug = false
    final val UseOpenMP = false
  }

  object Prefixes {
    final val ExtraArgument = "extra-argument."
    final val Nec = "spark.com.nec.spark.ncc."
    final val NecNonSpark = "ncc."
  }

  lazy val defaults: VeCompilerConfig = {
    System.getProperties.asScala
      .collect { case (k, v) if k.startsWith(Prefixes.NecNonSpark) && v != null =>
        k.drop(Prefixes.NecNonSpark.length) -> v
      }
      .foldLeft(VeCompilerConfig()) { case (veconf, (key, value)) => veconf.include(key, value) }
  }

  def fromSparkConf(config: SparkConf): VeCompilerConfig = {
    config
      .getAllWithPrefix(Prefixes.Nec)
      .foldLeft(defaults) { case (veconf, (key, value)) => veconf.include(key, value) }
  }
}

final case class VeCompilerConfig(nccPath: String = VeCompilerConfig.Defaults.NccPath,
                                  optimizationLevel: Int = VeCompilerConfig.Defaults.OptLevel,
                                  doDebug: Boolean = VeCompilerConfig.Defaults.DoDebug,
                                  useOpenMP: Boolean = VeCompilerConfig.Defaults.UseOpenMP,
                                  additionalOptions: Map[Int, String] = Map.empty) {
  def compilerFlags: Seq[String] = {
    // Defaults
    Seq(
      s"-O${optimizationLevel}",
      "-xc++",
      "-std=gnu++17",
      "-fPIC",
      // Optimizations used in Frovedis: -fno-defer-inline-template-instantiation -finline-functions -finline-max-depth=10 -msched-block
      "-fno-defer-inline-template-instantiation",
      "-finline-functions",
      "-finline-max-depth=10",
      "-msched-block",
      "-pthread",
      "-report-all",
      // "-ftrace",
      // "-lveftrace_p",
      "-fdiag-vector=2",
      "-Werror=return-type"
    ) ++
    // Options
    Seq(
      if (doDebug) Seq("-D", "DEBUG=1") else Seq.empty,
      if (useOpenMP) Seq("-fopenmp") else Seq.empty
    ).flatten ++
    // Additional options
    additionalOptions.toSeq.sortBy(_._1).map(_._2)
  }

  def include(key: String, value: String): VeCompilerConfig = {
    key match {
      case "o"      => copy(optimizationLevel = value.toInt)
      case "debug"  => copy(doDebug = Set("true", "1").contains(value.toLowerCase))
      case "openmp" => copy(useOpenMP = Set("true", "1").contains(value.toLowerCase))
      case "path"   => copy(nccPath = value)
      case key if key.startsWith(VeCompilerConfig.Prefixes.ExtraArgument) =>
        copy(additionalOptions =
          additionalOptions.updated(key.drop(VeCompilerConfig.Prefixes.ExtraArgument.length).toInt, value)
        )
      case other    => throw new MatchError(s"Unexpected key for NCC configuration: '${key}'")
    }
  }
}
