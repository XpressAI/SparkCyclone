package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.Sort.runOn
import com.nec.arrow.functions.Sort.sortJVM
import com.nec.aurora.Aurora
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.WithTestAllocator
import com.nec.cmake.functions.SortCSpec
import org.scalatest.freespec.AnyFreeSpec
import org.apache.arrow.vector.Float8Vector

import java.nio.file.Files

final class SortVeSpec extends AnyFreeSpec {
  "We can sort a list of ints" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)
    val soPath = VeKernelCompiler("avg", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, SortCSpec.SorterSource)
        .mkString("\n\n")
    )
    val proc = Aurora.veo_proc_create(0)
    val (sorted, expectedSorted) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {

          WithTestAllocator { alloc =>
            val outVector = new Float8Vector("value", alloc)
            val data: Seq[Double] = Seq(5, 1, 2, 34, 6)
            val lib: Long = Aurora.veo_load_library(proc, soPath.toString)
            try ArrowVectorBuilders.withDirectFloat8Vector(data) { vcv =>
              runOn(new VeArrowNativeInterface(proc, lib))(vcv, outVector)
              val res = (0 until outVector.getValueCount).map(i => outVector.get(i)).toList
              (res, sortJVM(vcv))
            } finally outVector.close()
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }
}
