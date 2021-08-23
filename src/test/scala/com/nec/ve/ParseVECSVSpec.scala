package com.nec.ve

import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.CsvParse
import com.nec.aurora.Aurora
import com.nec.cmake.functions.ParseCSVSpec.verifyOn
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

final class ParseVECSVSpec extends AnyFreeSpec {
  "We can do a run of CSV" ignore  {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectories(veBuildPath)
    val soPath = VeKernelCompiler(
      compilationPrefix = "csv",
      buildDir = veBuildPath,
      config = VeKernelCompiler.VeCompilerConfig.testConfig
        .copy(doDebug = false)
    ).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode)
        .mkString("\n\n")
    )

    val proc = Aurora.veo_proc_create(0)
    try {
      val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
      try {
        WithTestAllocator { alloc =>
          val outVector = new Float8Vector("value", alloc)
          val lib: Long = Aurora.veo_load_library(proc, soPath.toString)
          verifyOn(new VeArrowNativeInterface(proc, lib))
        }
      } finally Aurora.veo_context_close(ctx)
    } finally Aurora.veo_proc_destroy(proc)
  }
}
