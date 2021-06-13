package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.Sort.runOn
import com.nec.arrow.functions.Sort.sortJVM
import com.nec.aurora.Aurora
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Sum
import org.scalatest.freespec.AnyFreeSpec
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import java.nio.file.Files

final class SortVeSpec extends AnyFreeSpec {
  "We can sort a list of ints" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectory(veBuildPath)
    val oPath = veBuildPath.resolve("sort.o")
    val theCommand = List(
      "nc++",
      "-o",
      oPath.toString,
      "-I./src/main/resources/com/nec/arrow/functions/cpp",
      "-c",
      "./src/main/resources/com/nec/arrow/functions/cpp/sorter.cc",
      "-shared",
      "-I./src/main/resources/com/nec/arrow/functions",
      "-I./src/main/resources/com/nec/arrow/"
    )

    import scala.sys.process._
    info(theCommand.!!.toString)

    val soFile = veBuildPath.resolve("sort.so")
    val command2 = Seq("nc++", "-shared", "-pthread", "-o", soFile.toString, oPath.toString)
    info(command2.!!.toString)

    val proc = Aurora.veo_proc_create(0)
    val (sorted, expectedSorted) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          
          val alloc = new RootAllocator(Integer.MAX_VALUE)
          val outVector = new Float8Vector("value", alloc)
          val data: Seq[Double] = Seq(5, 1, 2, 34, 6)
          val lib: Long = Aurora.veo_load_library(proc, soFile.toString)
          ArrowVectorBuilders.withDirectFloat8Vector(data) { vcv =>
            runOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))(vcv, outVector)
            val res = (0 until outVector.getValueCount).map(i => outVector.get(i)).toList
            (res, sortJVM(vcv))
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }
}
