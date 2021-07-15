package com.nec.arrow.native
import com.nec.arrow.ArrowInterfaces.c_double_vector
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.arrow.VeArrowNativeInterfaceNumeric.nonNullDoubleVectorToByteBuffer
import com.nec.aurora.Aurora
import com.nec.ve.VeJavaContext
import com.sun.jna.Library
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.javacpp.LongPointer

import java.nio.file.Path
import scala.language.higherKinds

trait Float8GenericNativeIf[Container[_]] {
  def call(functionName: String, input: Container[Float8Vector]): Container[Float8Vector]
}

object Float8GenericNativeIf {

  final case class InVh[Contents](contents: Contents) {
    def inVe(implicit
      veJavaContext: VeJavaContext,
      writer: VectorEngineArrowWriter[Contents]
    ): InVe[Contents] = {
      writer.write(this)
    }
  }

  final case class InVe[Contents](vePointer: Long, count: Long, size: Long) {
    def inVh(implicit
      veJavaContext: VeJavaContext,
      reader: VectorEngineArrowReader[Contents]
    ): InVh[Contents] =
      reader.read(this)
  }
  final case class VeNativeIf(veJavaContext: VeJavaContext) extends Float8GenericNativeIf[InVe] {
    import veJavaContext._
    override def call(functionName: String, input: InVe[Float8Vector]): InVe[Float8Vector] = {
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
        require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")
        val veoPtr = ovb.getLong(0)
        val dataCount = ovb.getInt(8)
        outVector.count = dataCount
        outVector.data = veoPtr
        InVe(vePointer = outVector.data, count = outVector.count.toInt, size = outVector.size())
      } finally Aurora.veo_args_free(our_args)
    }

  }

  final case class NativeIf(libPath: Path) extends Float8GenericNativeIf[InVh] {
    override def call(functionName: String, input: InVh[Float8Vector]): InVh[Float8Vector] = {
      import scala.collection.JavaConverters._
      val nativeLibraryHandler =
        new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
      val nl = nativeLibraryHandler.getNativeLibrary
      val fn = nl.getFunction(functionName)
      val outputStruct = new non_null_double_vector()
      val invokeArgs: Array[AnyRef] = List(c_double_vector(input.contents), outputStruct).toArray
      fn.invokeLong(invokeArgs)
      val ra = new RootAllocator()
      val outputVector = new Float8Vector("result", ra)
      non_null_double_vector_to_float8Vector(outputStruct, outputVector)
      InVh(outputVector)
    }
  }

}
