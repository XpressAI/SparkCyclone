package com.nec.ve
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.aurora.Aurora
import com.nec.ve.FlexiPassingVESpec.Add1Mul2
import com.nec.ve.FlexiPassingVESpec.InVe
import com.nec.ve.FlexiPassingVESpec.InVh
import com.nec.ve.FlexiPassingVESpec.NativeIf
import com.nec.ve.FlexiPassingVESpec.RichFloat8
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths
import java.time.Instant
import scala.language.higherKinds

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

  trait NativeIf[Container[_]] {
    def call(functionName: String, input: Container[Float8Vector]): Container[Float8Vector]
  }

  /**
   * This is a combined function of Add1 and Mul2 demonstrating composition while processing the
   * data fully in the VE
   */
  object Add1Mul2 {
    def call[Container[_]](nativeIf: NativeIf[Container])(
      float8Vector: Container[Float8Vector]
    ): Container[Float8Vector] =
      Mul2.call(nativeIf)(Add1.call(nativeIf)(float8Vector))
  }

  object Add1 {
    def call[Container[_]](nativeIf: NativeIf[Container])(
      float8Vector: Container[Float8Vector]
    ): Container[Float8Vector] =
      nativeIf.call("add1", float8Vector)
  }

  object Mul2 {
    def call[Container[_]](nativeIf: NativeIf[Container])(
      float8Vector: Container[Float8Vector]
    ): Container[Float8Vector] =
      nativeIf.call("mul2", float8Vector)
  }

  final case class InVe[Contents](vePointer: Long, size: Long) {
    def inVh(implicit veJavaContext: VeJavaContext): InVh[Contents] = ???
  }

  final case class InVh[Contents](contents: Contents) {
    def inVe(implicit veJavaContext: VeJavaContext): InVe[Contents] = ???
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
          val nativeIf: NativeIf[InVe] = ???
          implicit val veJavaContext = new VeJavaContext(ctx, lib)
          ArrowVectorBuilders.withDirectFloat8Vector(List(1, 2, 3)) { vcv =>
            Add1Mul2.call(nativeIf)(InVh(vcv).inVe).inVh.contents.toList
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(result == List[Double](4, 6, 8))
  }
}
