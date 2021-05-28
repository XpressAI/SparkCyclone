package com.nec

import java.nio.file._

final case class VeCompiler(compilationPrefix: String, buildDir: Path) {
  require(buildDir.toAbsolutePath == buildDir, "Build dir should be absolute")
  def compile_c(sourceCode: String)(nccPath: String = "ncc"): Path = {
    if (!Files.exists(buildDir)) Files.createDirectories(buildDir)
    val cSource = buildDir.resolve(s"${compilationPrefix}.c")

    Files.write(cSource, sourceCode.getBytes())
    val oFile = buildDir.resolve(s"${compilationPrefix}.o")
    val soFile = buildDir.resolve(s"${compilationPrefix}.so")
    import scala.sys.process._
    val doDebug: Boolean = false
    val command = Seq(
      nccPath,
      "-O4",
      "-fpic",
      "-D",
      s"DEBUG=${if ( doDebug ) 1 else 0 }",
      "-fopenmp",
      "-mparallel",
      "-finline-functions",
      "-pthread",
      "-report-all",
      "-fdiag-vector=2",
      "-c",
      cSource.toString,
      "-o",
      oFile.toString
    )
    System.err.println(s"Will execute: ${command.mkString(" ")}")
    Process(command = command, cwd = buildDir.toFile).!!

    val command2 = Seq(nccPath, "-shared", "-pthread", "-o", soFile.toString, oFile.toString)
    System.err.println(s"Will execute: ${command2.mkString(" ")}")
    Process(command = command2, cwd = buildDir.toFile).!!

    soFile
  }
}
