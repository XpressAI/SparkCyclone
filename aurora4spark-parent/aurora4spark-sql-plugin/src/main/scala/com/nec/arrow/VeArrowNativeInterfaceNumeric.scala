package com.nec.arrow

import java.nio.ByteBuffer
import com.nec.arrow.ArrowTransferStructures.non_null_double_vector
import com.nec.aurora.Aurora
import com.nec.arrow.ArrowInterfaces.non_null_double_vector_to_float8Vector
import org.apache.arrow.vector.Float8Vector
import org.bytedeco.javacpp.LongPointer

final class VeArrowNativeInterfaceNumeric(
                                    proc: Aurora.veo_proc_handle,
                                    ctx: Aurora.veo_thr_ctxt,
                                    lib: Long
                                  ) extends ArrowNativeInterfaceNumeric {
  override def callFunction(
                             name: String,
                             inputArguments: List[Option[Float8Vector]],
                             outputArguments: List[Option[Float8Vector]]
                           ): Unit = VeArrowNativeInterfaceNumeric.executeVe(
    proc = proc,
    ctx = ctx,
    lib = lib,
    functionName = name,
    inputArguments = inputArguments,
    outputArguments = outputArguments
  )
}

object VeArrowNativeInterfaceNumeric {

  private def make_veo_double_vector(
                                      proc: Aurora.veo_proc_handle,
                                      float8Vector: Float8Vector
                                    ): non_null_double_vector = {
    val vcvr = new non_null_double_vector()
    vcvr.count = float8Vector.getValueCount
    vcvr.data = copyBufferToVe(proc, float8Vector.getDataBuffer.nioBuffer())
    vcvr
  }

  /** Take a vec, and rewrite the pointer to our local so we can read it */
  /** Todo deallocate from VE! unless we pass it onward */
  private def veo_read_non_null_double_vector(
                                            proc: Aurora.veo_proc_handle,
                                            vec: non_null_double_vector,
                                            byteBuffer: ByteBuffer
                                          ): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val dataCount = byteBuffer.getInt(8)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
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
  private def nonNullDoubleVectorToByteBuffer(double_vector: non_null_double_vector): ByteBuffer = {
    val v_bb = double_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, double_vector.data)
    v_bb.putInt(8, double_vector.count)
    v_bb
  }

  private def executeVe(
                         proc: Aurora.veo_proc_handle,
                         ctx: Aurora.veo_thr_ctxt,
                         lib: Long,
                         functionName: String,
                         inputArguments: List[Option[Float8Vector]],
                         outputArguments: List[Option[Float8Vector]]
                       ): Unit = {

    val our_args = Aurora.veo_args_alloc()
    try {
      inputArguments.zipWithIndex
        .collect { case (Some(doubleVector), idx) =>
          doubleVector -> idx
        }
        .foreach { case (doubleVector, index) =>
          val double_vector_raw = make_veo_double_vector(proc, doubleVector)

          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nonNullDoubleVectorToByteBuffer(double_vector_raw),
            12L
          )
        }

      val outputArgumentsVectors: List[(Float8Vector, Int)] = outputArguments
        .zipWithIndex
        .collect {
          case (Some(doubleVector), index) => doubleVector -> index
        }

      val outputArgumentsStructs: List[(non_null_double_vector, Int)] = outputArgumentsVectors.map {
        case (doubleVector, index) =>
          new non_null_double_vector(doubleVector.getValueCount) -> index
      }

      val outputArgumentsByteBuffers: List[(ByteBuffer, Int)] = outputArgumentsStructs.map {
        case (struct, index) =>
          nonNullDoubleVectorToByteBuffer(struct)-> index
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
          veo_read_non_null_double_vector(proc, non_null_int_vector, byteBuffer)
          non_null_double_vector_to_float8Vector(non_null_int_vector, intVector)
      }
    } finally Aurora.veo_args_free(our_args)
  }

}
