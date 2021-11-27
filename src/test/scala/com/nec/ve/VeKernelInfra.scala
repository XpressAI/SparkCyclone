package com.nec.ve

import com.nec.arrow.TransferDefinitions
import org.scalatest.Suite

import java.nio.file.{Path, Paths}
import java.time.Instant

trait VeKernelInfra { this: Suite =>

  def compiledWithHeaders[T](cCode: String)(f: Path => T): T = {
    withCompiled(s"${TransferDefinitions.TransferDefinitionsSourceCode}\n\n${cCode}\n")(f)
  }

  def withCompiled[T](cCode: String)(f: Path => T): T = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath =
      VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
        .compile_c(cCode)
    f(oPath)
  }
}
