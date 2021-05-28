package com.nec.arrow.native
import com.nec.ve.VeJavaContext
import com.nec.arrow.native.Float8GenericNativeIf.InVh
import com.nec.arrow.native.Float8GenericNativeIf.InVe
import com.nec.aurora.Aurora
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.javacpp.LongPointer

trait VectorEngineArrowWriter[T] {
  def write(inVh: InVh[T])(implicit veJavaContext: VeJavaContext): InVe[T]
}

object VectorEngineArrowWriter {
  implicit val float8Writer: VectorEngineArrowWriter[Float8Vector] =
    new VectorEngineArrowWriter[Float8Vector] {
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

}
