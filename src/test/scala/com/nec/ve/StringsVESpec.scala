package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders.withArrowStringVector
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Substr
import com.nec.aurora.Aurora
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichVarCharVector
import org.apache.arrow.vector.VarCharVector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant

final class StringsVESpec extends AnyFreeSpec {
  "Substr works" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)
    val soPath = VeKernelCompiler("substr", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Substr.SourceCode)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    try {
      val inputStrings = Seq("This is ", "Some sort", "of a test")
      withArrowStringVector(inputStrings) { vcv =>
        val outVector = new VarCharVector("value", vcv.getAllocator)
        val lib: Long = Aurora.veo_load_library(proc, soPath.toString)
        Substr.runOn(new VeArrowNativeInterfaceNumeric(proc, lib))(vcv, outVector, 1, 3)
        val expectedOutput = inputStrings.map(_.substring(1, 3)).toList

        try assert(outVector.toList == expectedOutput)
        finally outVector.close()
      }
    } finally Aurora.veo_proc_destroy(proc)
  }
}
