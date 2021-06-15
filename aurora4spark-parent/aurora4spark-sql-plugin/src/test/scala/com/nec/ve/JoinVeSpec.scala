package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders

import java.nio.file.Paths
import java.time.Instant
import JoinVeSpec.Join._
import com.nec.aurora.Aurora
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric
import com.nec.arrow.functions.Sum
import org.scalatest.freespec.AnyFreeSpec
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import java.nio.file.Files

object JoinVeSpec {

  import com.nec.arrow.ArrowNativeInterfaceNumeric
  import org.apache.arrow.vector.Float8Vector

  object Join {
    def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
      firstColumnVector: Float8Vector,
      outputVector: Float8Vector
    ): Unit = {

      outputVector.setValueCount(firstColumnVector.getValueCount())

      nativeInterface.callFunction(
        name = "join_doubles",
        inputArguments = List(Some(firstColumnVector), None),
        outputArguments = List(None, Some(outputVector))
      )
    }

    def joinJVM(inputVector: Float8Vector): List[Double] =
      (0 until inputVector.getValueCount).map { idx =>
        inputVector.get(idx)
      }.toList.sorted
  }

}
final class JoinVeSpec extends AnyFreeSpec {
  "We can join two lists" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectories(veBuildPath)
    val oPath = veBuildPath.resolve("join.o")
    val theCommand = List(
      "nc++",
      "-o",
      oPath.toString,
      "-I./src/main/resources/com/nec/arrow/functions/cpp",
      "-I./src/main/resources/com/nec/arrow/functions/cpp/frovedis",
      "-c",
      "./src/main/resources/com/nec/arrow/functions/cpp/joiner.cc",
      "-shared",
      "-I./src/main/resources/com/nec/arrow/functions",
      "-I./src/main/resources/com/nec/arrow/"
    )

    import scala.sys.process._
    info(theCommand.!!.toString)

    val soFile = veBuildPath.resolve("join.so")
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
            (res, joinJVM(vcv))
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }
}
