package com.nec.arrow

import com.nec.arrow.ArrowInterfaces.{nullable_bigint_vector_to_BigIntVector, nullable_double_vector_to_float8Vector, nullable_int_vector_to_IntVector, nullable_int_vector_to_SmallIntVector, nullable_varchar_vector_to_VarCharVector}
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper._
import com.nec.arrow.ArrowNativeInterface.NativeArgument.{VectorInputNativeArgument, VectorOutputNativeArgument}
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.{BigIntVectorOutputWrapper, Float8VectorOutputWrapper, IntVectorOutputWrapper, SmallIntVectorOutputWrapper, VarCharVectorOutputWrapper}
import com.nec.arrow.ArrowTransferStructures._
import com.nec.arrow.VeArrowNativeInterface.{Cleanup, copyBufferToVe, requireOk}
import com.nec.aurora.Aurora
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector._
import sun.nio.ch.DirectBuffer
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable

import org.apache.spark.sql.util.ArrowUtilsExposed

object VeArrowTransfers extends LazyLogging {

  def transferOutput(
    proc: Aurora.veo_proc_handle,
    our_args: Aurora.veo_args,
    transferBack: mutable.Buffer[() => Unit],
    wrapper: VectorOutputNativeArgument.OutputVectorWrapper,
    index: Int
  )(implicit cleanup: Cleanup): Unit = {
    wrapper match {
      case Float8VectorOutputWrapper(doubleVector) =>
        val structVector = new nullable_double_vector()
        val byteBuffer = nullableDoubleVectorToByteBuffer(structVector)
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
        transferBack.append(() => {
          veo_read_nullable_double_vector(proc, structVector, byteBuffer)
          nullable_double_vector_to_float8Vector(structVector, doubleVector)
        })
      case IntVectorOutputWrapper(intWrapper) =>
        val structVector = new nullable_int_vector()
        val byteBuffer = nullableIntVectorToByteBuffer(structVector)
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
        transferBack.append(() => {
          veo_read_nullable_int_vector(proc, structVector, byteBuffer)
          nullable_int_vector_to_IntVector(structVector, intWrapper)
        })
      case BigIntVectorOutputWrapper(bigIntWrapper) =>
        val structVector = new nullable_bigint_vector()
        val byteBuffer = nullableBigintVectorToByteBuffer(structVector)
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
        transferBack.append(() => {
          veo_read_nullable_bigint_vector(proc, structVector, byteBuffer)
          nullable_bigint_vector_to_BigIntVector(structVector, bigIntWrapper)
        })
      case VarCharVectorOutputWrapper(varCharVector) =>
        val structVector = new nullable_varchar_vector()
        val byteBuffer = nullableVarCharVectorVectorToByteBuffer(structVector)
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
        transferBack.append(() => {
          veo_read_nullable_varchar_vector(proc, structVector, byteBuffer)
          nullable_varchar_vector_to_VarCharVector(structVector, varCharVector)
        })
      case SmallIntVectorOutputWrapper(smallIntVector) =>
        val structVector = new nullable_int_vector()
        val byteBuffer = nullableIntVectorToByteBuffer(structVector)
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
        transferBack.append(() => {
          veo_read_nullable_int_vector(proc, structVector, byteBuffer)
          nullable_int_vector_to_SmallIntVector(structVector, smallIntVector)
        })
    }
  }

  def transferInput(
    proc: Aurora.veo_proc_handle,
    our_args: Aurora.veo_args,
    wrapper: VectorInputNativeArgument.InputVectorWrapper,
    index: Int
  )(implicit cleanup: Cleanup): Unit = {
    wrapper match {
      case DateDayVectorInputWrapper(dateDayVector) =>
        val int_vector_raw = make_veo_date_vector(proc, dateDayVector)
        requireOk(
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToByteBuffer(int_vector_raw),
            20L
          )
        )
      case ByteBufferInputWrapper(byteBuffer, size) =>
        val wr = make_veo_string_of_byteBuffer(proc, byteBuffer, size)
        requireOk(Aurora.veo_args_set_stack(our_args, 0, index, stringToByteBuffer(wr), 12L))

      case StringInputWrapper(stringValue) =>
        val wr = make_veo_string(proc, stringValue)
        requireOk(Aurora.veo_args_set_stack(our_args, 0, index, stringToByteBuffer(wr), 12L))
      case Float8VectorInputWrapper(doubleVector) =>
        val double_vector_raw = make_veo_double_vector(proc, doubleVector)
        requireOk(
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableDoubleVectorToByteBuffer(double_vector_raw),
            20L
          )
        )
      case IntVectorInputWrapper(intVector) =>
        val int_vector_raw = make_veo_int_vector(proc, intVector)
        requireOk(
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToByteBuffer(int_vector_raw),
            20L
          )
        )
      case VarCharVectorInputWrapper(varcharVector) =>
        val varchar_vector_raw = make_veo_varchar_vector(proc, varcharVector)

        requireOk(
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableVarCharVectorVectorToByteBuffer(varchar_vector_raw),
            32L
          )
        )

      case BigIntVectorInputWrapper(longVector) =>
        val long_vector_raw = make_veo_bigint_vector(proc, longVector)

        requireOk(
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableBigintVectorToByteBuffer(long_vector_raw),
            20L
          )
        )
      case SmallIntVectorInputWrapper(smallIntVector) =>
        val int_vector_raw = make_veo_int_vector(proc, smallIntVector)
        requireOk(
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToByteBuffer(int_vector_raw),
            20L
          )
        )
    }
  }

  private def make_veo_double_vector(proc: Aurora.veo_proc_handle, float8Vector: Float8Vector)(
    implicit cleanup: Cleanup
  ): nullable_double_vector = {
    val keyName = "double_" + float8Vector.getName + "_" + float8Vector.getDataBuffer.capacity()
    logger.debug(s"Copying Buffer to VE for $keyName")
    val vcvr = new nullable_double_vector()
    vcvr.count = float8Vector.getValueCount
    vcvr.data = copyBufferToVe(proc, float8Vector.getDataBuffer.nioBuffer())(cleanup)
    vcvr.validityBuffer = copyBufferToVe(proc, float8Vector.getValidityBuffer.nioBuffer())(cleanup)

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

  private def make_veo_int_vector(proc: Aurora.veo_proc_handle, intVector: IntVector)(implicit
    cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int2_" + intVector.getName + "_" + intVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
    vcvr.count = intVector.getValueCount
    vcvr.data = copyBufferToVe(proc, intVector.getDataBuffer.nioBuffer())(cleanup)
    vcvr.validityBuffer = copyBufferToVe(proc, intVector.getValidityBuffer.nioBuffer())(cleanup)

    vcvr
  }

  private def make_veo_int_vector(proc: Aurora.veo_proc_handle, smallIntVector: SmallIntVector)(implicit
                                                                                      cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int2_" + smallIntVector.getName + "_" + smallIntVector.getDataBuffer.capacity()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(smallIntVector.getValueCount)

    (0 until smallIntVector.getValueCount)

      .foreach{
        case idx if(!smallIntVector.isNull(idx)) => intVector.set(idx, smallIntVector.get(idx).toInt)
        case idx => intVector.setNull(idx)
      }
    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
    vcvr.count = intVector.getValueCount
    vcvr.data = copyBufferToVe(proc, intVector.getDataBuffer.nioBuffer())(cleanup)
    vcvr.validityBuffer = copyBufferToVe(proc, intVector.getValidityBuffer.nioBuffer())(cleanup)

    vcvr
  }

  private def make_veo_date_vector(proc: Aurora.veo_proc_handle, dateDayVector: DateDayVector)(
    implicit cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int2_" + dateDayVector.getName + "_" + dateDayVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
    vcvr.count = dateDayVector.getValueCount
    vcvr.data = copyBufferToVe(proc, dateDayVector.getDataBuffer.nioBuffer())(cleanup)
    vcvr.validityBuffer = copyBufferToVe(proc, dateDayVector.getValidityBuffer.nioBuffer())(cleanup)

    vcvr
  }

  private def make_veo_varchar_vector(proc: Aurora.veo_proc_handle, varcharVector: VarCharVector)(
    implicit cleanup: Cleanup
  ): nullable_varchar_vector = {
    val keyName =
      "varchar_" + varcharVector.getName + "_" + varcharVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_varchar_vector()
    vcvr.count = varcharVector.getValueCount
    vcvr.size = varcharVector.getOffsetBuffer.getInt(4 * vcvr.count)
    vcvr.data = copyBufferToVe(proc, varcharVector.getDataBuffer.nioBuffer())(cleanup)
    vcvr.offsets = copyBufferToVe(proc, varcharVector.getOffsetBuffer.nioBuffer())(cleanup)
    vcvr
  }

  private def make_veo_bigint_vector(proc: Aurora.veo_proc_handle, bigintVector: BigIntVector)(
    implicit cleanup: Cleanup
  ): nullable_bigint_vector = {
    val keyName = "biging_" + bigintVector.getName + "_" + bigintVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_bigint_vector()
    vcvr.count = bigintVector.getValueCount
    vcvr.data = copyBufferToVe(proc, bigintVector.getDataBuffer.nioBuffer())(cleanup)
    vcvr.validityBuffer = copyBufferToVe(proc, bigintVector.getValidityBuffer.nioBuffer())(cleanup)

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
    requireOk(
      Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    )
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_nullable_double_vector(
    proc: Aurora.veo_proc_handle,
    vec: nullable_double_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val validityPtr = byteBuffer.getLong(8)
    val dataCount = byteBuffer.getInt(16)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    val validityTarget = ByteBuffer.allocateDirect(dataCount)

    requireOk {
      Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
      Aurora.veo_read_mem(
        proc,
        new org.bytedeco.javacpp.Pointer(validityTarget),
        validityPtr,
        dataCount
      )
    }
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    vec.validityBuffer = validityTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()

    cleanup.add(veoPtr, dataSize)
    cleanup.add(validityPtr, dataCount)
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
    requireOk(
      Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    )
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_nullable_int_vector(
    proc: Aurora.veo_proc_handle,
    vec: nullable_int_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val validityPtr = byteBuffer.getLong(8)
    val dataCount = byteBuffer.getInt(16)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    val vhValidityTarget = ByteBuffer.allocateDirect(dataCount)
    requireOk {
      Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
      Aurora.veo_read_mem(
        proc,
        new org.bytedeco.javacpp.Pointer(vhValidityTarget),
        validityPtr,
        dataCount
      )

    }
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    vec.validityBuffer = vhValidityTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()

    cleanup.add(veoPtr, dataSize)
    cleanup.add(validityPtr, dataSize)
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
    requireOk(
      Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    )
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_nullable_bigint_vector(
    proc: Aurora.veo_proc_handle,
    vec: nullable_bigint_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val validityPtr = byteBuffer.getLong(8)
    val dataCount = byteBuffer.getInt(16)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    val validityTarget = ByteBuffer.allocateDirect(dataCount)

    requireOk {
      Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veoPtr, dataSize)
    }

    requireOk {
      Aurora.veo_read_mem(
        proc,
        new org.bytedeco.javacpp.Pointer(validityTarget),
        validityPtr,
        dataCount
      )
    }
    vec.count = dataCount
    vec.data = vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    vec.validityBuffer = validityTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address()

    cleanup.add(veoPtr, dataSize)
    cleanup.add(validityPtr, dataSize)
  }

  private def veo_read_nullable_varchar_vector(
    proc: Aurora.veo_proc_handle,
    vec: nullable_varchar_vector,
    byteBuffer: ByteBuffer
  )(implicit cleanup: Cleanup): Unit = {

    /** Get data size */
    val dataSize = byteBuffer.getInt(24)
    vec.size = dataSize

    /** Get data count */
    val dataCount = byteBuffer.getInt(28)
    vec.count = dataCount

    /** Transfer the data */
    val dataPtr = byteBuffer.getLong(0)
    val vhTargetData = ByteBuffer.allocateDirect(dataSize)
    requireOk {
      Aurora
        .veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTargetData), dataPtr, dataSize)
    }
    vec.data = vhTargetData.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(dataPtr, dataSize)

    /** Transfer the offsets */
    val offsetsPtr = byteBuffer.getLong(8)
    val vhTargetOffsets = ByteBuffer.allocateDirect((dataCount + 1) * 4)
    requireOk {
      Aurora
        .veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTargetOffsets), offsetsPtr, (dataCount + 1) * 4)
    }
    vec.offsets = vhTargetOffsets.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(offsetsPtr, (dataCount + 1) * 4)

    /** Transfer the validity buffer */
    val validityPtr = byteBuffer.getLong(16)
    val vhValidity = ByteBuffer.allocateDirect(Math.ceil(vec.count / 8.0).toInt)
    requireOk {
      Aurora
        .veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhValidity), validityPtr, Math.ceil(vec.count / 8.0).toInt)
    }
    vec.validityBuffer = vhValidity.asInstanceOf[sun.nio.ch.DirectBuffer].address()
    cleanup.add(validityPtr, Math.ceil(vec.count / 8.0).toInt)
  }

  def stringToByteBuffer(str_buf: non_null_c_bounded_string): ByteBuffer = {
    val v_bb = str_buf.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, str_buf.data)
    v_bb.putInt(8, str_buf.length)
    v_bb
  }

  def nullableDoubleVectorToByteBuffer(double_vector: nullable_double_vector): ByteBuffer = {
    val v_bb = double_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, double_vector.data)
    v_bb.putLong(8, double_vector.validityBuffer)
    v_bb.putInt(16, double_vector.count)
    v_bb
  }

  def nullableBigintVectorToByteBuffer(bigint_vector: nullable_bigint_vector): ByteBuffer = {
    val v_bb = bigint_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, bigint_vector.data)
    v_bb.putLong(8, bigint_vector.validityBuffer)
    v_bb.putInt(16, bigint_vector.count)
    v_bb
  }

  def nullableIntVectorToByteBuffer(int_vector: nullable_int_vector): ByteBuffer = {
    val v_bb = int_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, int_vector.data)
    v_bb.putLong(8, int_vector.validityBuffer)
    v_bb.putInt(16, int_vector.count)
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

  def nullableVarCharVectorVectorToByteBuffer(
    varchar_vector: nullable_varchar_vector
  ): ByteBuffer = {
    val v_bb = varchar_vector.getPointer.getByteBuffer(0, 32)
    v_bb.putLong(0, varchar_vector.data)
    v_bb.putLong(8, varchar_vector.offsets)
    v_bb.putLong(16, varchar_vector.validityBuffer)
    v_bb.order(ByteOrder.LITTLE_ENDIAN)
    v_bb.putInt(24, varchar_vector.size)
    v_bb.putInt(28, varchar_vector.count)
    v_bb
  }
}
