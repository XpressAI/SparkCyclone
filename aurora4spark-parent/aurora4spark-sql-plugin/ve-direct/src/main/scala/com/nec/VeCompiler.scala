package com.nec

import java.nio.file._

final case class VeCompiler(compilationPrefix: String, buildDir: Path) {
  require(buildDir.toAbsolutePath == buildDir, "Build dir should be absolute")
  def compile_c(sourceCode: String): Path = {
    if (!Files.exists(buildDir)) Files.createDirectory(buildDir)
    val cSource = buildDir.resolve(s"${compilationPrefix}.c")

    Files.write(cSource, sourceCode.getBytes())
    val oFile = buildDir.resolve(s"${compilationPrefix}.o")
    val soFile = buildDir.resolve(s"${compilationPrefix}.so")
    import scala.sys.process._
    Seq(
      "ncc",
      "-O2",
      "-fpic",
      "-pthread",
      "-report-all",
      "-fdiag-vector=2",
      "-c",
      cSource.toString,
      "-o",
      oFile.toString
    ).!!
    Seq("ncc", "-shared", "-pthread", "-o", soFile.toString, oFile.toString).!!
    soFile
  }
}
