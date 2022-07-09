package io.sparkcyclone.native.compiler

import io.sparkcyclone.native.NativeFunction
import io.sparkcyclone.native.code.{CFunction2, CodeLines}
import io.sparkcyclone.spark.codegen.CFunctionGeneration.CFunction
import java.nio.file.{Path, Paths}
import java.time.Instant
import org.scalatest.Suite

trait VeKernelInfra { this: Suite =>
  protected implicit def kernelInfra: VeKernelInfra = this

  def withCompiled[T](func: CFunction, name: String)(thunk: Path => T): T = {
    withCompiled(func.toCodeLinesHeaderPtr(name).cCode)(thunk)
  }

  def withCompiled[T](func: CFunction2)(thunk: Path => T): T = {
    withCompiled(func.toCodeLinesWithHeaders.cCode)(thunk)
  }

  def withCompiled[T](func: NativeFunction)(thunk: Path => T): T = {
    withCompiled(func.codelines.cCode)(thunk)
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
