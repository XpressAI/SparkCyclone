package com.nec.ve
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.aurora.Aurora
import com.nec.ve.FlexiPassingVESpec.Add1Mul2
import com.nec.ve.FlexiPassingVESpec.RichFloat8
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import java.time.Instant
import scala.language.higherKinds
import com.nec.arrow.native.Float8GenericNativeIf
import com.nec.arrow.native.Float8GenericNativeIf.InVh
import com.nec.arrow.native.Float8GenericNativeIf.VeNativeIf

/**
 * Here, we will enable us to do passing of an output to another input without having to copy
 * back to VH. This will be a huge performance boost as everything stays on the VE!
 */
object FlexiPassingVESpec {

  val Sources: String = {
    val source = scala.io.Source.fromInputStream(
      getClass.getResourceAsStream("/com/nec/arrow/functions/flexi.c")
    )
    try source.mkString
    finally source.close()
  }

  /**
   * This is a combined function of Add1 and Mul2 demonstrating composition while processing the
   * data fully in the VE
   */
  object Add1Mul2 {
    def call[Container[_]](
      nativeIf: Float8GenericNativeIf[Container]
    )(float8Vector: Container[Float8Vector]): Container[Float8Vector] =
      MulN
        .call(nativeIf)(
          float8Vector = AddN.call(nativeIf)(float8Vector = float8Vector, n = 1),
          n = 2
        )
  }

  object AddN {
    def call[Container[_]](
      nativeIf: Float8GenericNativeIf[Container]
    )(float8Vector: Container[Float8Vector], n: Double): Container[Float8Vector] =
      nativeIf.call(functionName = "add_n", input = (float8Vector, n))
  }

  object MulN {
    def call[Container[_]](
      nativeIf: Float8GenericNativeIf[Container]
    )(float8Vector: Container[Float8Vector], n: Double): Container[Float8Vector] =
      nativeIf.call(functionName = "mul_n", input = (float8Vector, n))
  }

  implicit class RichFloat8(float8Vector: Float8Vector) {
    def toList: List[Double] =
      (0 until float8Vector.getValueCount).map { idx =>
        float8Vector.getValueAsDouble(idx)
      }.toList
  }

}
final class FlexiPassingVESpec extends AnyFreeSpec {
  "It works" in {

    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeKernelCompiler("wc", veBuildPath).compile_c(
      List(TransferDefinitions.TransferDefinitionsSourceCode, FlexiPassingVESpec.Sources)
        .mkString("\n\n")
    )

    val proc = Aurora.veo_proc_create(0)
    val result =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          implicit val veJavaContext = new VeJavaContext(proc, ctx, lib)
          val nativeIf = VeNativeIf(veJavaContext)
          ArrowVectorBuilders.withDirectFloat8Vector(List(1, 2, 3)) { vcv =>
            assert(InVh(vcv).inVe.inVh.contents.toList == vcv.toList)
            Add1Mul2.call(nativeIf)(InVh(vcv).inVe).inVh.contents.toList
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(result == List[Double](4, 6, 8))
  }
}
