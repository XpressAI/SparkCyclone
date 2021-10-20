package com.nec.spark

import com.nec.spark.VeKernelCompilerConfigSpec.{defnsStringValue, stringValue}
import com.nec.ve.VeKernelCompiler.VeCompilerConfig
import org.scalatest.freespec.AnyFreeSpec
import org.apache.spark.SparkConf

object VeKernelCompilerConfigSpec {

  private def compilerConfig: VeCompilerConfig = VeCompilerConfig.fromSparkConf(
    new SparkConf().setAll(
      List(
        "spark.com.nec.spark.ncc.debug" -> "true",
        "spark.com.nec.spark.ncc.o" -> "3",
        "spark.com.nec.spark.ncc.openmp" -> "false",
        "spark.com.nec.spark.ncc.extra-argument.0" -> "-X",
        "spark.com.nec.spark.ncc.extra-argument.1" -> "-Y"
      )
    )
  )

  private def stringValue = compilerConfig.compilerArguments.toString

}
final class VeKernelCompilerConfigSpec extends AnyFreeSpec {
  "it captures DEBUG option" in {
    assert(stringValue.contains("DEBUG=1"))
  }
  "it captures Optimization override" in {
    assert(stringValue.contains("-O3"))
  }
  "It captures disabling OpenMP" in {
    assert(!stringValue.contains("openmp"))
  }
  "It can include extra arguments" in {
    assert(stringValue.contains("-X, -Y"))
  }
}
