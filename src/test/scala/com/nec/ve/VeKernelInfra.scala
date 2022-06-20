package com.nec.ve

import com.nec.native.compiler.VeKernelCompiler
import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, KeyHeaders}
import com.nec.spark.agile.core.CFunction2
import org.scalatest.Suite

import java.nio.file.{Path, Paths}
import java.time.Instant

trait VeKernelInfra { this: Suite =>

  protected implicit def kernelInfra: VeKernelInfra = this

  def compiledWithHeaders[T](cCode: CFunction, name: String)(f: Path => T): T = {
    withCompiled(cCode.toCodeLinesHeaderPtr(name).cCode)(f)
  }

  def compiledWithHeaders[T](func: CFunction2)(f: Path => T): T = {
    withCompiled(func.toCodeLinesWithHeaders.cCode)(f)
  }

  def withCompiled[T](cCode: String)(f: Path => T): T = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath =
      VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
        .compile(cCode)
    f(oPath)
  }
}
