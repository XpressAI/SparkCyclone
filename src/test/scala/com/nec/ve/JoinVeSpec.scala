package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions

import java.nio.file.Paths
import java.time.Instant
import com.nec.arrow.functions.Join._
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.WithTestAllocator
import com.nec.cmake.functions.JoinCSpec.JoinerSource
import com.typesafe.scalalogging.LazyLogging
import com.nec.spark.SparkAdditions
import org.scalatest.freespec.AnyFreeSpec
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, ConfigMap}

final class JoinVeSpec
  extends AnyFreeSpec
  with BeforeAndAfterAll
  with SparkAdditions
  with LazyLogging {

  implicit class RichVeProc(proc: veo_proc_handle) {
    def ensureOk(): veo_proc_handle = {
      require(proc != null, "Proc could not be allocated, got null")
      require(proc.address() != 0, s"Address for 0 for proc was ${proc}")
      logger.info(s"VE Process = ${proc}")
      proc
    }
  }

  private var initialized = false
  private lazy val proc = {
    initialized = true
    veo.veo_proc_create(0).ensureOk()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (initialized) {
      veo.veo_proc_destroy(proc)
    }
  }

  "We can join two lists" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler("avg", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, JoinerSource)
        .mkString("\n\n")
    )
    val proc = veo.veo_proc_create(0)
    val (sorted, expectedSorted) =
      try {

        WithTestAllocator { alloc =>
          val outVector = new Float8Vector("value", alloc)
          val firstColumn: Seq[Double] = Seq(5, 1, 2, 34, 6)
          val secondColumn: Seq[Double] = Seq(100, 15, 92, 331, 49)
          val firstColumnKeys: Seq[Int] = Seq(1, 2, 3, 4, 5)
          val secondColumnKeys: Seq[Int] = Seq(4, 2, 5, 200, 800)
          val lib: Long = veo.veo_load_library(proc, oPath.toString)
          try ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
            ArrowVectorBuilders.withDirectFloat8Vector(secondColumn) { secondColumnVec =>
              ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
                ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
                  runOn(new VeArrowNativeInterface(proc, lib))(
                    firstColumnVec,
                    secondColumnVec,
                    firstKeysVec,
                    secondKeysVec,
                    outVector
                  )
                  val res = (0 until outVector.getValueCount)
                    .map(i => outVector.get(i))
                    .toList
                    .splitAt(outVector.getValueCount / 2)
                  val joinResult = res._1.zip(res._2)
                  (
                    joinResult,
                    joinJVM(firstColumnVec, secondColumnVec, firstKeysVec, secondKeysVec)
                  )
                }
              }
            }
          } finally outVector.close()
        }

      } finally veo.veo_proc_destroy(proc)

    assert(sorted.nonEmpty)
    assert(sorted == expectedSorted)
  }

  "We can join two smaller lists" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val oPath = VeKernelCompiler("avg", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, JoinerSource)
        .mkString("\n\n")
    )
    val proc = veo.veo_proc_create(0)
    try {

      WithTestAllocator { alloc =>
        val outVector = new Float8Vector("value", alloc)
        val firstColumn: Seq[Double] = Seq(1, 2)
        val secondColumn: Seq[Double] = Seq(2, 3)
        val firstColumnKeys: Seq[Int] = Seq(1, 2)
        val secondColumnKeys: Seq[Int] = Seq(1, 2)
        val lib: Long = veo.veo_load_library(proc, oPath.toString)
        try ArrowVectorBuilders.withDirectFloat8Vector(firstColumn) { firstColumnVec =>
          ArrowVectorBuilders.withDirectFloat8Vector(secondColumn) { secondColumnVec =>
            ArrowVectorBuilders.withDirectIntVector(firstColumnKeys) { firstKeysVec =>
              ArrowVectorBuilders.withDirectIntVector(secondColumnKeys) { secondKeysVec =>
                runOn(new VeArrowNativeInterface(proc, lib))(
                  firstColumnVec,
                  secondColumnVec,
                  firstKeysVec,
                  secondKeysVec,
                  outVector
                )
                val res = (0 until outVector.getValueCount)
                  .map(i => outVector.get(i))
                  .toList

                assert(res == List(1.0, 2.0, 2.0, 3.0))
              }
            }
          }
        } finally {
          outVector.close()
        }
      }
    } finally veo.veo_proc_destroy(proc)

  }
}
