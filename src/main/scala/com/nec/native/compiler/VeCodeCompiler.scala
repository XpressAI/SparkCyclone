package com.nec.native.compiler

import com.nec.native.{NativeCodeCompiler, NativeFunction}
import java.nio.file.Path
import com.typesafe.scalalogging.LazyLogging

final case class OnDemandVeCodeCompiler(cwd: Path,
                                        config: VeCompilerConfig = VeCompilerConfig.defaults)
                                        extends NativeCodeCompiler with LazyLogging {
  logger.info(s"VE Kernel Compiler configuration: ${config}")

  def build(functions: Seq[NativeFunction]): Map[Int, Path] = {
    val soPath = build(combinedCode(functions))
    functions.map(f => (f.hashId -> soPath)).toMap
  }

  def build(code: String): Path = {
    val prefix = s"_spark_${code.hashCode}"

    val tmp = cwd.resolve(s"${prefix}.so").normalize.toAbsolutePath

    if (tmp.toFile.exists) {
      logger.debug(s".SO File with hash code ${code.hashCode} already exists; returning the pre-compiled .SO: ${tmp}")
      tmp

    } else {
      VeKernelCompiler(prefix, cwd.normalize.toAbsolutePath, config).compile(code)
    }
  }
}
