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
import org.apache.arrow.vector.{Float8Vector, IntVector}
import java.nio.file.Files

import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.{Float8VectorWrapper, IntVectorWrapper}

object JoinVeSpec {

  import com.nec.arrow.ArrowNativeInterfaceNumeric
  import org.apache.arrow.vector.Float8Vector

  object Join {
    def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
      leftValuesVector: Float8Vector,
      rightValuesVector: Float8Vector,
      leftKeyVector: IntVector,
      rightKeyVector: IntVector,
      outputVector: Float8Vector
    ): Unit = {

      nativeInterface.callFunction(
        name = "join_doubles",
        inputArguments = List(
          Some(Float8VectorWrapper(leftValuesVector)),
          Some(Float8VectorWrapper(rightValuesVector)),
          Some(IntVectorWrapper(leftKeyVector)),
          Some(IntVectorWrapper(rightKeyVector)),
          None
        ),
        outputArguments = List(None, None , None, None, Some(outputVector))
      )
    }

    def joinJVM(leftColumn: Float8Vector, rightColumn: Float8Vector,
                leftKey: IntVector, rightKey: IntVector): List[(Double, Double)] = {
      val leftColVals = (0 until leftColumn.getValueCount).map(idx => leftColumn.get(idx))
      val rightColVals = (0 until rightColumn.getValueCount).map(idx => rightColumn.get(idx))
      val leftKeyVals = (0 until leftKey.getValueCount).map(idx => leftKey.get(idx))
      val rightKeyVals = (0 until rightKey.getValueCount).map(idx => rightKey.get(idx))
      val leftMap = leftKeyVals.zip(leftColVals).toMap
      val rightMap = rightKeyVals.zip(rightColVals).toMap
      val joinedKeys = leftKeyVals.filter(key => rightMap.contains(key))
      joinedKeys.map(key => leftMap(key)).zip(
        joinedKeys.map(key => rightMap(key))
      ).toList
    }
  }

}
final class JoinVeSpec extends AnyFreeSpec {
  "We can join two lists" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    Files.createDirectories(veBuildPath)
    val oPath = veBuildPath.resolve("join.so")
    val theCommand = List(
      "g++",
      "-o",
      oPath.toString,
      "-I./src/main/resources/com/nec/arrow/functions/cpp",
      "-I./src/main/resources/com/nec/arrow/functions/cpp/frovedis",
      "-I./src/main/resources/com/nec/arrow/functions/cpp/frovedis/dataframe",
      "-shared",
      "-I./src/main/resources/com/nec/arrow/functions",
      "-I./src/main/resources/com/nec/arrow/",
      "-xc++",
      "./src/main/resources/com/nec/arrow/functions/cpp/joiner.cc"
    )

    import scala.sys.process._
    info(theCommand.!!)

    val proc = Aurora.veo_proc_create(0)
    val (sorted, expectedSorted) =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          
          val alloc = new RootAllocator(Integer.MAX_VALUE)
          val outVector = new Float8Vector("value", alloc)
          val firstColumn: Seq[Double] = Seq(5, 1, 2, 34, 6)
          val secondColumn: Seq[Double] = Seq(100, 15, 92, 331, 49)
          val firstColumnKeys: Seq[Int] = Seq(1, 2, 3, 4, 5)
          val secondColumnKeys: Seq[Int] = Seq(4, 2, 5, 200, 800)
          val lib: Long = Aurora.veo_load_library(proc, oPath.toString)
          ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
            ArrowVectorBuilders.withDirectFloat8Vector(secondColumn){ secondColumnVec =>
              ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
                ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
                  runOn(new VeArrowNativeInterfaceNumeric(proc, ctx, lib))(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec, outVector)
                  val res = (0 until outVector.getValueCount).map(i => outVector.get(i)).toList
                    .splitAt(outVector.getValueCount/2)
                  val joinResult = res._1.zip(res._2)
                  (joinResult, joinJVM(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec))
                }
                }
              }
            }

        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }
}
