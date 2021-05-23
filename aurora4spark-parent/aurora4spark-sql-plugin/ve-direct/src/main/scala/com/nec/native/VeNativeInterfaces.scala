package com.nec.native

import org.apache.arrow.vector.{IntVector, VarCharVector}
import com.nec.CountStringsLibrary.{non_null_int_vector, varchar_vector_raw}
import com.nec.WordCount.non_null_int_vector_to_IntVector
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

  /** Workaround - not sure why it does not work immediately */
  private def varcharVectorRawToByteBuffer(varchar_vector_raw: varchar_vector_raw): ByteBuffer = {
    val v_bb = varchar_vector_raw.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, varchar_vector_raw.data)
    v_bb.putLong(8, varchar_vector_raw.offsets)
    v_bb.putInt(16, varchar_vector_raw.count)
    v_bb
  }

  def executeVe(
    proc: Aurora.veo_proc_handle,
    ctx: Aurora.veo_thr_ctxt,
    lib: Long,
    functionName: String,
    inputArguments: List[Option[VarCharVector]],
    outputArguments: List[Option[IntVector]]
  ): Unit = {

    val our_args = Aurora.veo_args_alloc()
    try {
      inputArguments.zipWithIndex
        .collect { case (Some(inputVarChar), idx) =>
          inputVarChar -> idx
        }
        .foreach { case (inputVarChar, index) =>
          val varchar_vector_raw = make_veo_varchar_vector(proc, inputVarChar)
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            varcharVectorRawToByteBuffer(varchar_vector_raw),
            20L
          )
        }

      val outputArgumentsVectors: List[(IntVector, Int)] = outputArguments.zipWithIndex.collect {
        case (Some(intVector), index) => intVector -> index
      }

      val outputArgumentsStructs: List[(non_null_int_vector, Int)] = outputArgumentsVectors.map {
        case (intVector, index) =>
          new non_null_int_vector() -> index
      }

      val outputArgumentsByteBuffers: List[(ByteBuffer, Int)] = outputArgumentsStructs.map {
        case (struct, index) =>
          struct.getPointer.getByteBuffer(0, 12) -> index
      }

      outputArgumentsByteBuffers.foreach { case (byteBuffer, index) =>
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
      }

      val req_id = Aurora.veo_call_async_by_name(ctx, lib, functionName, our_args)
      val fnCallResult = new LongPointer(8)
      val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
      require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      (outputArgumentsVectors.zip(outputArgumentsStructs).zip(outputArgumentsByteBuffers)).foreach {
        case (((intVector, _), (non_null_int_vector, _)), (byteBuffer, _)) =>
          veo_read_non_null_int_vector(proc, non_null_int_vector, byteBuffer)
          non_null_int_vector_to_IntVector(non_null_int_vector, intVector)
      }
    } finally Aurora.veo_args_free(our_args)
  }

}
