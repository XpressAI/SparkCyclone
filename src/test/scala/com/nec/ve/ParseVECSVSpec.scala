package com.nec.ve

import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.CsvParse
import com.nec.aurora.Aurora
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.verifyOn
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

final class ParseVECSVSpec extends AnyFreeSpec {
  "We can sort a list of ints" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)
    val soPath = VeKernelCompiler("csv", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode)
        .mkString("\n\n")
    )

    val proc = Aurora.veo_proc_create(0)
    try {
      val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
      try {
        val alloc = new RootAllocator(Integer.MAX_VALUE)
        val outVector = new Float8Vector("value", alloc)
        val data: Seq[Double] = Seq(5, 1, 2, 34, 6)
        val lib: Long = Aurora.veo_load_library(proc, soPath.toString)
        verifyOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))
      } finally Aurora.veo_context_close(ctx)
    } finally Aurora.veo_proc_destroy(proc)
  }
  "Through Arrow, it works" in {
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode)
        .mkString("\n\n")
    )
  }
}
