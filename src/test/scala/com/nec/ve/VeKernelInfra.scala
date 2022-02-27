package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunctionGeneration.{CFunction, KeyHeaders}
import org.scalatest.Suite

import java.nio.file.{Path, Paths}
import java.time.Instant

trait VeKernelInfra { this: Suite =>

  protected implicit def kernelInfra: VeKernelInfra = this

  def compiledWithHeaders[T](cCode: CFunction, name: String)(f: Path => T): T = {
    withCompiled(cCode.toCodeLinesHeaderPtr(name).cCode)(f)
  }

  def compiledWithHeaders[T](cCode: CFunction2, name: String)(f: Path => T): T = {
    withCompiled(CodeLines.from(KeyHeaders, cCode.toCodeLines(name)).cCode)(f)
  }

  def withCompiled[T](cCode: String)(f: Path => T): T = {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath =
      VeKernelCompiler(s"${getClass.getSimpleName.replaceAllLiterally("$", "")}", veBuildPath)
        .compile_c(cCode)
    f(oPath)
  }
}
