package com.nec.native

import org.apache.arrow.vector.VarCharVector
import com.nec.CountStringsLibrary.{non_null_int_vector, varchar_vector_raw}
import com.nec.aurora.Aurora
import com.sun.jna.Pointer
import org.bytedeco.javacpp.LongPointer

import java.nio.ByteBuffer

object VeNativeInterfaces {

  def make_veo_varchar_vector[T](
    proc: Aurora.veo_proc_handle,
    varCharVector: VarCharVector
  ): varchar_vector_raw = {
    val vcvr = new varchar_vector_raw()
    vcvr.count = varCharVector.getValueCount
    vcvr.data = copyBufferToVe(proc, varCharVector.getDataBuffer.nioBuffer())
    vcvr.offsets = copyBufferToVe(proc, varCharVector.getOffsetBuffer.nioBuffer())
    vcvr
  }

  /** Take a vec, and rewrite the pointer to our local so we can read it */
  /** Todo deallocate from VE! unless we pass it onward */
  def veo_read_non_null_int_vector(
    proc: Aurora.veo_proc_handle,
    vec: non_null_int_vector,
    byteBuffer: ByteBuffer
  ): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val dataCount = byteBuffer.getInt(8)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    vec.count = dataCount
    vec.data = new Pointer(vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address())
  }

  def copyBufferToVe(proc: Aurora.veo_proc_handle, byteBuffer: ByteBuffer): Long = {
    val veInputPointer = new LongPointer(8)
    val size = byteBuffer.capacity()
    Aurora.veo_alloc_mem(proc, veInputPointer, size)
    Aurora.veo_write_mem(
      proc,
      /** after allocating, this pointer now contains a value of the VE storage address * */
      veInputPointer.get(),
      new org.bytedeco.javacpp.Pointer(byteBuffer),
      size
    )
    veInputPointer.get()
  }

}
