package com.nec.arrow

import java.nio.ByteBuffer
import com.nec.arrow.ArrowTransferStructures._
import com.nec.aurora.Aurora
import com.nec.arrow.ArrowInterfaces._
import org.apache.arrow.vector._
import org.bytedeco.javacpp.LongPointer
import com.nec.arrow.ArrowNativeInterfaceNumeric._
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._
import com.sun.jna.Structure
import com.typesafe.scalalogging.LazyLogging
import sun.nio.ch.DirectBuffer

final class VeArrowNativeInterfaceNumeric(
  proc: Aurora.veo_proc_handle,
  ctx: Aurora.veo_thr_ctxt,
  lib: Long
) extends ArrowNativeInterfaceNumeric {
  override def callFunctionGen(
    name: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = VeArrowNativeInterfaceNumeric.executeVe(
    proc = proc,
    ctx = ctx,
    lib = lib,
    functionName = name,
    inputArguments = inputArguments,
    outputArguments = outputArguments
  )
}

object VeArrowNativeInterfaceNumeric extends LazyLogging {

  private def make_veo_double_vector(proc: Aurora.veo_proc_handle, float8Vector: Float8Vector)(
    implicit cleanup: Cleanup
  ): non_null_double_vector = {
    val vcvr = new non_null_double_vector()
    vcvr.count = float8Vector.getValueCount
    vcvr.data = copyBufferToVe(
      proc,
      float8Vector.getDataBuffer.nioBuffer(),
      Some(float8Vector.getDataBuffer.capacity())
    )
    vcvr
  }

  private def make_veo_string(proc: Aurora.veo_proc_handle, string: String)(implicit
    cleanup: Cleanup
  ): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
    val theBuf = ByteBuffer
      .allocateDirect(string.length)
      .put(string.getBytes())
    theBuf.position(0)
    vc.length = string.length
    vc.data = copyBufferToVe(proc, theBuf)
    vc
  }

  private def make_veo_string_of_byteBuffer(
    proc: Aurora.veo_proc_handle,
    byteBuffer: ByteBuffer,
    size: Int
  )(implicit cleanup: Cleanup): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
    vc.length = size
    byteBuffer.position(0)
    val theBuf =
      if (byteBuffer.isInstanceOf[DirectBuffer]) byteBuffer
      else {
        ByteBuffer
          .allocateDirect(size)
          .put(byteBuffer)
      }
    theBuf.position(0)
    vc.data = copyBufferToVe(proc, theBuf, Some(size))
    vc
  }

  private def make_veo_int2_vector(proc: Aurora.veo_proc_handle, intVector: IntVector)(implicit
    cleanup: Cleanup
  ): non_null_int2_vector = {
    val vcvr = new non_null_int2_vector()
    vcvr.count = intVector.getValueCount
    vcvr.data = copyBufferToVe(proc, intVector.getDataBuffer.nioBuffer())
    vcvr
  }

  /** Take a vec, and rewrite the pointer to our local so we can read it */
  /** Todo deallocate from VE! unless we pass it onward */
  private def veo_read_non_null_double_vector(
    proc: Aurora.veo_proc_handle,
    vec: non_null_double_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val dataCount = byteBuffer.getInt(8)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_non_null_int2_vector(
    proc: Aurora.veo_proc_handle,
    vec: non_null_int2_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val dataCount = byteBuffer.getInt(8)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_non_null_bigint_vector(
    proc: Aurora.veo_proc_handle,
    vec: non_null_bigint_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val dataCount = byteBuffer.getInt(8)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_non_null_varchar_vector(
    proc: Aurora.veo_proc_handle,
    vec: non_null_varchar_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    /* data = size 8, offsets = size 8, size = size 4, count = size 4; so: 0, 8, 16, 20 */
    val veoPtrData = byteBuffer.getLong(0)
    val dataSize = byteBuffer.getInt(16)
    val vhTargetData = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTargetData), veoPtrData, dataSize)
    vec.size = dataSize
    vec.data = vhTargetData.asInstanceOf[sun.nio.ch.DirectBuffer].address()

    val veoPtrOffsets = byteBuffer.getLong(8)
    val dataCount = byteBuffer.getInt(20)
    val vhTargetOffsets = ByteBuffer.allocateDirect((dataCount + 1) * 4)
    Aurora.veo_read_mem(
      proc,
      new org.bytedeco.javacpp.Pointer(vhTargetOffsets),
      veoPtrOffsets,
      (dataCount + 1) * 4
    )
    vec.count = dataCount
    vec.offsets = vhTargetOffsets.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtrOffsets, (dataCount + 1) * 4)
    cleanup.add(veoPtrData, dataSize)
  }

  class Cleanup(var items: List[Long] = Nil) {
    def add(long: Long, size: Long): Unit = items = {
      logger.debug(s"Adding to clean-up: ${long}, ${size} bytes")
      long :: items
    }
  }

  def copyBufferToVe(
    proc: Aurora.veo_proc_handle,
    byteBuffer: ByteBuffer,
    len: Option[Long] = None
  )(implicit cleanup: Cleanup): Long = {
    val veInputPointer = new LongPointer(8)

    /** No idea why Arrow in some cases returns a ByteBuffer with 0-capacity, so we have to pass a length explicitly! */
    val size = len.getOrElse(byteBuffer.capacity().toLong)
    Aurora.veo_alloc_mem(proc, veInputPointer, size)
    Aurora.veo_write_mem(
      proc,
      /** after allocating, this pointer now contains a value of the VE storage address * */
      veInputPointer.get(),
      new org.bytedeco.javacpp.Pointer(byteBuffer),
      size
    )
    veInputPointer.get()
    val ptr = veInputPointer.get()
    cleanup.add(ptr, size)
    ptr
  }

  def stringToByteBuffer(str_buf: non_null_c_bounded_string): ByteBuffer = {
    val v_bb = str_buf.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, str_buf.data)
    v_bb.putInt(8, str_buf.length)
    v_bb
  }

  def nonNullDoubleVectorToByteBuffer(double_vector: non_null_double_vector): ByteBuffer = {
    val v_bb = double_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, double_vector.data)
    v_bb.putInt(8, double_vector.count)
    v_bb
  }

  def nonNullInt2VectorToByteBuffer(int_vector: non_null_int2_vector): ByteBuffer = {
    val v_bb = int_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, int_vector.data)
    v_bb.putInt(8, int_vector.count)
    v_bb
  }

  def nonNullBigIntVectorToByteBuffer(bigint_vector: non_null_bigint_vector): ByteBuffer = {
    val v_bb = bigint_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, bigint_vector.data)
    v_bb.putInt(8, bigint_vector.count)
    v_bb
  }

  def nonNullVarCharVectorVectorToByteBuffer(
    varchar_vector: non_null_varchar_vector
  ): ByteBuffer = {
    val v_bb = varchar_vector.getPointer.getByteBuffer(0, 24)
    v_bb.putLong(0, varchar_vector.data)
    v_bb.putLong(8, varchar_vector.offsets)
    v_bb.putInt(16, varchar_vector.size)
    v_bb.putInt(20, varchar_vector.count)
    v_bb
  }

  private def executeVe(
    proc: Aurora.veo_proc_handle,
    ctx: Aurora.veo_thr_ctxt,
    lib: Long,
    functionName: String,
    inputArguments: List[Option[SupportedVectorWrapper]],
    outputArguments: List[Option[SupportedVectorWrapper]]
  ): Unit = {
    assert(lib > 0, s"Expected lib to be >0, was $lib")
    val our_args = Aurora.veo_args_alloc()
    implicit val cleanup: Cleanup = new Cleanup()
    try {
      inputArguments.zipWithIndex
        .collect { case (Some(doubleVector), idx) =>
          doubleVector -> idx
        }
        .foreach {
          case (ByteBufferWrapper(byteBuffer, size), index) =>
            val wr = make_veo_string_of_byteBuffer(proc, byteBuffer, size)

            Aurora.veo_args_set_stack(our_args, 0, index, stringToByteBuffer(wr), 12L)
          case (StringWrapper(stringValue), index) =>
            val wr = make_veo_string(proc, stringValue)

            Aurora.veo_args_set_stack(our_args, 0, index, stringToByteBuffer(wr), 12L)
          case (Float8VectorWrapper(doubleVector), index) =>
            val double_vector_raw = make_veo_double_vector(proc, doubleVector)

            Aurora.veo_args_set_stack(
              our_args,
              0,
              index,
              nonNullDoubleVectorToByteBuffer(double_vector_raw),
              12L
            )
          case (IntVectorWrapper(intVector), index) =>
            val int_vector_raw = make_veo_int2_vector(proc, intVector)

            Aurora.veo_args_set_stack(
              our_args,
              0,
              index,
              nonNullInt2VectorToByteBuffer(int_vector_raw),
              12L
            )
        }

      val outputArgumentsVectorsDouble: List[(SupportedVectorWrapper, Int)] =
        outputArguments.zipWithIndex
          .collect { case (Some(vec), index) =>
            vec -> index
          }

      val outputArgumentsZipped: List[(SupportedVectorWrapper, Structure, ByteBuffer, Int)] =
        outputArgumentsVectorsDouble
          .map {
            case (vec @ Float8VectorWrapper(doubleVector), index) =>
              val structVector = new non_null_double_vector(doubleVector.getValueCount)
              val byteBuffer = nonNullDoubleVectorToByteBuffer(structVector)
              (vec, structVector, byteBuffer, index)
            case (vec @ IntVectorWrapper(intWrapper), index) =>
              val structVector = new non_null_int2_vector()
              val byteBuffer = nonNullInt2VectorToByteBuffer(structVector)
              (vec, structVector, byteBuffer, index)
            case (vec @ BigIntVectorWrapper(bigIntWrapper), index) =>
              val structVector = new non_null_bigint_vector()
              val byteBuffer = nonNullBigIntVectorToByteBuffer(structVector)
              (vec, structVector, byteBuffer, index)
            case (vec @ VarCharVectorWrapper(varCharVector), index) =>
              val structVector = new non_null_varchar_vector()
              val byteBuffer = nonNullVarCharVectorVectorToByteBuffer(structVector)
              (vec, structVector, byteBuffer, index)
          }

      outputArgumentsZipped.foreach { case (vec, struct, byteBuffer, index) =>
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
      }

      val startTime = System.currentTimeMillis()
      val uuid = java.util.UUID.randomUUID()
      logger.debug(s"[$uuid] Starting VE call to ${functionName}...")
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, functionName, our_args)
      logger.debug(s"[$uuid] Async call to '${functionName}' completed (waiting for result) ")
      val fnCallResult = new LongPointer(8)
      val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
      val time = System.currentTimeMillis() - startTime
      logger.debug(
        s"[$uuid] Got result from VE call to ${functionName}: ${callRes}. Took ${time}ms"
      )
      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function ${functionName}; inputs: ${inputArguments}; outputs: ${outputArguments}"
      )
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      outputArgumentsZipped
        .foreach {
          case (Float8VectorWrapper(vec), struct: non_null_double_vector, byteBuff, _) =>
            veo_read_non_null_double_vector(proc, struct, byteBuff)
            non_null_double_vector_to_float8Vector(struct, vec)
          case (IntVectorWrapper(vec), struct: non_null_int2_vector, byteBuff, _) =>
            veo_read_non_null_int2_vector(proc, struct, byteBuff)
            non_null_int2_vector_to_IntVector(struct, vec)
          case (BigIntVectorWrapper(vec), struct: non_null_bigint_vector, byteBuff, _) =>
            veo_read_non_null_bigint_vector(proc, struct, byteBuff)
            non_null_bigint_vector_to_bigIntVector(struct, vec)
          case (VarCharVectorWrapper(vec), struct: non_null_varchar_vector, byteBuff, _) =>
            veo_read_non_null_varchar_vector(proc, struct, byteBuff)
            non_null_varchar_vector_to_VarCharVector(struct, vec)
        }
    } finally {
      cleanup.items.foreach(ptr => Aurora.veo_free_mem(proc, ptr))
      Aurora.veo_args_free(our_args)
    }
  }

}
