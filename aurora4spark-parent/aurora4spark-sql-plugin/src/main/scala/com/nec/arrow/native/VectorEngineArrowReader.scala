package com.nec.arrow.native
import com.nec.LocationPointer
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.ve.VeJavaContext
import com.nec.arrow.native.Float8GenericNativeIf.InVh
import com.nec.arrow.native.Float8GenericNativeIf.InVe
import com.nec.aurora.Aurora
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.javacpp.DoublePointer

import java.nio.ByteBuffer

trait VectorEngineArrowReader[T] {
  def read(inVe: InVe[T])(implicit veJavaContext: VeJavaContext): InVh[T]
}
object VectorEngineArrowReader {

  implicit val float8Reader: VectorEngineArrowReader[Float8Vector] =
    new VectorEngineArrowReader[Float8Vector] {
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
        val lp = new DoublePointer(new LocationPointer(nndv.data, inVe.size))
        val outputVector = new Float8Vector("count", ra)
        non_null_double_vector_to_float8Vector(nndv, outputVector)
        InVh(outputVector)
      }
    }
}
