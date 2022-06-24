package com.nec.native.compiler

import com.nec.spark.agile.core.{CFunction2, CodeLines}
import com.nec.spark.agile.CFunctionGeneration.CFunction
import java.nio.file.{Path, Paths}
import java.time.Instant
import org.scalatest.Suite

trait VeKernelInfra { this: Suite =>
  protected implicit def kernelInfra: VeKernelInfra = this

  def compiledWithHeaders[T](func: CFunction, name: String)(thunk: Path => T): T = {
    withCompiled(func.toCodeLinesHeaderPtr(name).cCode)(thunk)
  }

  def compiledWithHeaders[T](func: CFunction2)(thunk: Path => T): T = {
    withCompiled(func.toCodeLinesWithHeaders.cCode)(thunk)
  }

  def withCompiled[T](code: String)(thunk: Path => T): T = {
    val libpath = VeKernelCompilation(
      s"${getClass.getSimpleName.replaceAllLiterally("$", "")}",
      Paths.get("target", "ve", s"${Instant.now.toEpochMilli}").normalize.toAbsolutePath,
      code
    ).run

    thunk(libpath)
  }
}
