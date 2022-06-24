package com.nec.native.compiler

import com.nec.native.{NativeCodeCompiler, NativeFunction}
import java.nio.file.{Files, Path}
import com.typesafe.scalalogging.LazyLogging

final case class OnDemandVeCodeCompiler(cwd: Path,
                                        config: VeCompilerConfig = VeCompilerConfig.defaults)
                                        extends NativeCodeCompiler with LazyLogging {
  logger.info(s"VE kernel compiler configuration: ${config}")

  if (! Files.exists(cwd)) {
    logger.info(s"Creating build directory for the VE kernel compiler: ${cwd}")
    Files.createDirectories(cwd, VeKernelCompilation.FileAttributes)
  }

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
      VeKernelCompilation(prefix, cwd.normalize.toAbsolutePath, code, config).run
    }
  }
}
