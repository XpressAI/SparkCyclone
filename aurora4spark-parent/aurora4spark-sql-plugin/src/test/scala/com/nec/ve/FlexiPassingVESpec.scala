package com.nec.ve
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterfaceNumeric.nonNullDoubleVectorToByteBuffer
import com.nec.aurora.Aurora
import com.nec.ve.FlexiPassingVESpec.Add1Mul2
import com.nec.ve.FlexiPassingVESpec.InVe
import com.nec.ve.FlexiPassingVESpec.InVh
import com.nec.ve.FlexiPassingVESpec.NativeIf
import com.nec.ve.FlexiPassingVESpec.RichFloat8
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.javacpp.LongPointer
import org.scalatest.freespec.AnyFreeSpec

import java.nio.ByteBuffer
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

  trait Reader[T] {
    def read(inVe: InVe[T])(implicit veJavaContext: VeJavaContext): InVh[T]
  }

  implicit val float8Reader: Reader[Float8Vector] = new Reader[Float8Vector] {
    override def read(
      inVe: InVe[Float8Vector]
    )(implicit veJavaContext: VeJavaContext): InVh[Float8Vector] = {
      val vhTarget = ByteBuffer.allocateDirect(inVe.size.toInt)
      Aurora.veo_read_mem(
        veJavaContext.proc,
        new org.bytedeco.javacpp.Pointer(vhTarget),
        inVe.vePointer,
        inVe.size
      )
      val nndv = new non_null_double_vector()
      val ra = new RootAllocator()

      nndv.count = inVe.count.toInt
      nndv.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()

      val outputVector = new Float8Vector("count", ra)
      non_null_double_vector_to_float8Vector(nndv, outputVector)
      InVh(outputVector)
    }
  }

  trait Writer[T] {
    def write(inVh: InVh[T])(implicit veJavaContext: VeJavaContext): InVe[T]
  }

  implicit val float8Writer: Writer[Float8Vector] = new Writer[Float8Vector] {
    override def write(
      inVh: InVh[Float8Vector]
    )(implicit veJavaContext: VeJavaContext): InVe[Float8Vector] = {
      import veJavaContext._
      val veInputPointer = new LongPointer(8)
      val float8Vector = inVh.contents

      val byteBuffer = float8Vector.getDataBuffer.nioBuffer()
      val size = byteBuffer.capacity()
      Aurora.veo_alloc_mem(proc, veInputPointer, size)
      Aurora.veo_write_mem(
        proc,
        /** after allocating, this pointer now contains a value of the VE storage address * */
        veInputPointer.get(),
        new org.bytedeco.javacpp.Pointer(byteBuffer),
        size
      )
      InVe(veInputPointer.get(), float8Vector.getValueCount, size)
    }
  }

  final case class InVe[Contents](vePointer: Long, count: Long, size: Long) {
    def inVh(implicit veJavaContext: VeJavaContext, reader: Reader[Contents]): InVh[Contents] =
      reader.read(this)
  }

  final case class InVh[Contents](contents: Contents) {
    def inVe(implicit veJavaContext: VeJavaContext, writer: Writer[Contents]): InVe[Contents] = {
      writer.write(this)
    }
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
          val nativeIf: NativeIf[InVe] = new NativeIf[InVe] {
            override def call(
              functionName: String,
              input: InVe[Float8Vector]
            ): InVe[Float8Vector] = {
              val our_args = Aurora.veo_args_alloc()
              try {

                def make_veo_double_vector: non_null_double_vector = {
                  val vcvr = new non_null_double_vector()
                  vcvr.count = input.count.toInt
                  vcvr.data = input.vePointer
                  vcvr
                }

                val double_vector_raw = make_veo_double_vector

                Aurora.veo_args_set_stack(
                  our_args,
                  0,
                  0,
                  nonNullDoubleVectorToByteBuffer(double_vector_raw),
                  12L
                )

                val outVector = new non_null_double_vector()
                val ovb = nonNullDoubleVectorToByteBuffer(outVector)

                Aurora.veo_args_set_stack(our_args, 1, 1, ovb, ovb.limit())

                val req_id = Aurora.veo_call_async_by_name(ctx, lib, functionName, our_args)
                val fnCallResult = new LongPointer(8)
                val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
                require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
                require(
                  fnCallResult.get() == 0L,
                  s"Expected 0, got ${fnCallResult.get()} back instead."
                )

                InVe(
                  vePointer = outVector.data,
                  count = outVector.count.toInt,
                  size = outVector.count.toInt * 4
                )
              } finally Aurora.veo_args_free(our_args)
            }

          }
          implicit val veJavaContext = new VeJavaContext(proc, ctx, lib)
          ArrowVectorBuilders.withDirectFloat8Vector(List(1, 2, 3)) { vcv =>
            Add1Mul2.call(nativeIf)(InVh(vcv).inVe).inVh.contents.toList
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    assert(result == List[Double](4, 6, 8))
  }
}
