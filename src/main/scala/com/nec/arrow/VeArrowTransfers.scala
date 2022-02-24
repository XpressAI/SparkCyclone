/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.nec.arrow

import com.nec.arrow.ArrowInterfaces.{
  intCharsFromVarcharVector,
  lengthsFromVarcharVector,
  nullable_bigint_vector_to_BigIntVector,
  nullable_bigint_vector_to_TimeStampVector,
  nullable_double_vector_to_float8Vector,
  nullable_int_vector_to_BitVector,
  nullable_int_vector_to_IntVector,
  nullable_int_vector_to_SmallIntVector,
  nullable_varchar_vector_to_VarCharVector,
  startsFromVarcharVector
}
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper._
import com.nec.arrow.ArrowNativeInterface.NativeArgument.{
  VectorInputNativeArgument,
  VectorOutputNativeArgument
}
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorOutputNativeArgument.OutputVectorWrapper.{
  BigIntVectorOutputWrapper,
  BitVectorOutputWrapper,
  Float8VectorOutputWrapper,
  IntVectorOutputWrapper,
  SmallIntVectorOutputWrapper,
  TimeStampVectorOutputWrapper,
  VarCharVectorOutputWrapper
}
import com.nec.arrow.TransferDefinitions._
import com.nec.arrow.VeArrowNativeInterface.{copyPointerToVe, requireOk, requirePositive, Cleanup}
import com.typesafe.scalalogging.LazyLogging
import org.apache.arrow.vector._

import scala.collection.mutable
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.javacpp.DoublePointer
import org.bytedeco.javacpp.LongPointer
import org.bytedeco.javacpp.IntPointer
import org.bytedeco.javacpp.ShortPointer
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_args
import org.bytedeco.veoffload.veo_proc_handle
import sun.nio.ch.DirectBuffer

import java.nio.ByteBuffer

object VeArrowTransfers extends LazyLogging {

  def transferOutput(
    proc: veo_proc_handle,
    our_args: veo_args,
    wrapper: VectorOutputNativeArgument.OutputVectorWrapper,
    index: Int
  )(implicit cleanup: Cleanup): () => Unit = {
    wrapper match {
      case Float8VectorOutputWrapper(doubleVector) =>
        val structVector = new nullable_double_vector()
        val bytePointer = nullableDoubleVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_double_vector(proc, structVector, bytePointer)
          nullable_double_vector_to_float8Vector(structVector, doubleVector)
        })

      case IntVectorOutputWrapper(intWrapper) =>
        val structVector = new nullable_int_vector()
        val bytePointer = nullableIntVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_int_vector(proc, structVector, bytePointer)
          nullable_int_vector_to_IntVector(structVector, intWrapper)
        })
      case BigIntVectorOutputWrapper(bigIntWrapper) =>
        val structVector = new nullable_bigint_vector()
        val bytePointer = nullableBigintVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_bigint_vector(proc, structVector, bytePointer)
          nullable_bigint_vector_to_BigIntVector(structVector, bigIntWrapper)
        })
      case VarCharVectorOutputWrapper(varCharVector) =>
        val structVector = new nullable_varchar_vector()
        val bytePointer = nullableVarCharVectorVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_varchar_vector(proc, structVector, bytePointer)
          nullable_varchar_vector_to_VarCharVector(structVector, varCharVector)
        })
      case SmallIntVectorOutputWrapper(smallIntVector) =>
        val structVector = new nullable_int_vector()
        val bytePointer = nullableIntVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_int_vector(proc, structVector, bytePointer)
          nullable_int_vector_to_SmallIntVector(structVector, smallIntVector)
        })
      case BitVectorOutputWrapper(bitVector) =>
        val structVector = new nullable_int_vector()
        val bytePointer = nullableIntVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_int_vector(proc, structVector, bytePointer)
          nullable_int_vector_to_BitVector(structVector, bitVector)
        })
      case TimeStampVectorOutputWrapper(tsWrapper) =>
        val structVector = new nullable_bigint_vector()
        val bytePointer = nullableBigintVectorToBytePointer(structVector)
        veo.veo_args_set_stack(our_args, 1, index, bytePointer, bytePointer.limit())

        (() => {
          veo_read_nullable_bigint_vector(proc, structVector, bytePointer)
          nullable_bigint_vector_to_TimeStampVector(structVector, tsWrapper)
        })
    }
  }

  def transferInput(
    proc: veo_proc_handle,
    our_args: veo_args,
    wrapper: VectorInputNativeArgument.InputVectorWrapper,
    index: Int
  )(implicit cleanup: Cleanup): Unit = {
    wrapper match {
      case DateDayVectorInputWrapper(dateDayVector) =>
        val int_vector_raw = make_veo_date_vector(proc, dateDayVector)
        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToBytePointer(int_vector_raw),
            20L
          )
        )
      case BytePointerInputWrapper(bytePointer, size) =>
        val wr = make_veo_string_of_bytePointer(proc, bytePointer, size)
        requireOk(veo.veo_args_set_stack(our_args, 0, index, stringToBytePointer(wr), 12L))

      case StringInputWrapper(stringValue) =>
        val wr = make_veo_string(proc, stringValue)
        requireOk(veo.veo_args_set_stack(our_args, 0, index, stringToBytePointer(wr), 12L))
      case Float8VectorInputWrapper(doubleVector) =>
        val double_vector_raw = make_veo_double_vector(proc, doubleVector)
        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableDoubleVectorToBytePointer(double_vector_raw),
            20L
          )
        )
      case IntVectorInputWrapper(intVector) =>
        val int_vector_raw = make_veo_int_vector(proc, intVector)
        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToBytePointer(int_vector_raw),
            20L
          )
        )
      case VarCharVectorInputWrapper(varcharVector) =>
        val varchar_vector_raw = make_veo_varchar_vector(proc, varcharVector)

        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableVarCharVectorVectorToBytePointer(varchar_vector_raw),
            32L
          )
        )

      case BigIntVectorInputWrapper(longVector) =>
        val long_vector_raw = make_veo_bigint_vector(proc, longVector)

        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableBigintVectorToBytePointer(long_vector_raw),
            20L
          )
        )
      case SmallIntVectorInputWrapper(smallIntVector) =>
        val int_vector_raw = make_veo_int_vector(proc, smallIntVector)
        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToBytePointer(int_vector_raw),
            20L
          )
        )
      case BitVectorInputWrapper(bitVector) =>
        val bit_vector_raw = make_veo_int_vector(proc, bitVector)
        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableIntVectorToBytePointer(bit_vector_raw),
            20L
          )
        )
      case TimeStampVectorInputWrapper(tsVector) =>
        val long_vector_raw = make_veo_bigint_vector(proc, tsVector)

        requireOk(
          veo.veo_args_set_stack(
            our_args,
            0,
            index,
            nullableBigintVectorToBytePointer(long_vector_raw),
            20L
          )
        )
    }
  }

  private def make_veo_double_vector(proc: veo_proc_handle, float8Vector: Float8Vector)(implicit
    cleanup: Cleanup
  ): nullable_double_vector = {
    val keyName = "double_" + float8Vector.getName + "_" + float8Vector.getDataBuffer.capacity()
    logger.debug(s"Copying Buffer to VE for $keyName")
    val vcvr = new nullable_double_vector()
      .count(float8Vector.getValueCount)
      .data(new DoublePointer(copyPointerToVe(proc, new BytePointer(float8Vector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(float8Vector.getValidityBuffer.nioBuffer()))(cleanup)))

    vcvr
  }

  private def make_veo_string(proc: veo_proc_handle, string: String)(implicit
    cleanup: Cleanup
  ): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()

    val thePtr = new BytePointer(string.getBytes("UTF-32LE"):_*)
      
    vc
      .length(string.length)
      .data(new BytePointer(copyPointerToVe(proc, thePtr)))
  }

  private def make_veo_string_of_bytePointer(
    proc: veo_proc_handle,
    bytePointer: BytePointer,
    size: Int
  )(implicit cleanup: Cleanup): non_null_c_bounded_string = {
    bytePointer.position(0)
    val vc = new non_null_c_bounded_string()
      .length(size)
      .data(new BytePointer(copyPointerToVe(proc, bytePointer, Some(size))))
    vc
  }

  private def make_veo_int_vector(proc: veo_proc_handle, intVector: IntVector)(implicit
    cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int2_" + intVector.getName + "_" + intVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
      .count(intVector.getValueCount)
      .data(new IntPointer(copyPointerToVe(proc, new BytePointer(intVector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(intVector.getValidityBuffer.nioBuffer()))(cleanup)))

    vcvr
  }

  private def make_veo_int_vector(proc: veo_proc_handle, smallIntVector: SmallIntVector)(implicit
    cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int2_" + smallIntVector.getName + "_" + smallIntVector.getDataBuffer.capacity()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(smallIntVector.getValueCount)

    (0 until smallIntVector.getValueCount)
      .foreach {
        case idx if (!smallIntVector.isNull(idx)) =>
          intVector.set(idx, smallIntVector.get(idx).toInt)
        case idx => intVector.setNull(idx)
      }
    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
      .count(intVector.getValueCount)
      .data(new IntPointer(copyPointerToVe(proc, new BytePointer(intVector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(intVector.getValidityBuffer.nioBuffer()))(cleanup)))

    vcvr
  }

  private def make_veo_int_vector(proc: veo_proc_handle, bitVector: BitVector)(implicit
    cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int1_" + bitVector.getName + "_" + bitVector.getDataBuffer.capacity()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(bitVector.getValueCount)

    (0 until bitVector.getValueCount)
      .foreach {
        case idx if (!bitVector.isNull(idx)) => intVector.set(idx, bitVector.get(idx))
        case idx                             => intVector.setNull(idx)
      }
    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
      .count(intVector.getValueCount)
      .data(new IntPointer(copyPointerToVe(proc, new BytePointer(intVector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(intVector.getValidityBuffer.nioBuffer()))(cleanup)))

    vcvr
  }

  private def make_veo_date_vector(proc: veo_proc_handle, dateDayVector: DateDayVector)(implicit
    cleanup: Cleanup
  ): nullable_int_vector = {
    val keyName = "int2_" + dateDayVector.getName + "_" + dateDayVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_int_vector()
      .count(dateDayVector.getValueCount)
      .data(new IntPointer(copyPointerToVe(proc, new BytePointer(dateDayVector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(dateDayVector.getValidityBuffer.nioBuffer()))(cleanup)))
    
    vcvr
  }

  private def make_veo_varchar_vector(proc: veo_proc_handle, varcharVector: VarCharVector)(implicit
    cleanup: Cleanup
  ): nullable_varchar_vector = {
    val keyName =
      "varchar_" + varcharVector.getName + "_" + varcharVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")
    val dataBuff = intCharsFromVarcharVector(varcharVector)
    val startsBuff = startsFromVarcharVector(varcharVector)
    val lengthsBuff = lengthsFromVarcharVector(varcharVector)

    val vcvr = new nullable_varchar_vector()
      .count(varcharVector.getValueCount)
      .dataSize(dataBuff.capacity().toInt / 4)
      .data(new IntPointer(copyPointerToVe(
      proc = proc,
      bytePointer = new BytePointer(dataBuff),
      len = Some(dataBuff.capacity())
    )(cleanup)))
      .offsets(new IntPointer(copyPointerToVe(
      proc,
      new BytePointer(startsBuff),
      len = Some(startsBuff.capacity())
    )(cleanup)))
      .lengths(new IntPointer(copyPointerToVe(
      proc,
      new BytePointer(lengthsBuff),
      len = Some(lengthsBuff.capacity())
    )(cleanup)))

    vcvr
  }

  private def make_veo_bigint_vector(proc: veo_proc_handle, bigintVector: BigIntVector)(implicit
    cleanup: Cleanup
  ): nullable_bigint_vector = {
    val keyName = "bigint_" + bigintVector.getName + "_" + bigintVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_bigint_vector()
      .count(bigintVector.getValueCount)
      .data(new LongPointer(copyPointerToVe(proc, new BytePointer(bigintVector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(bigintVector.getValidityBuffer.nioBuffer()))(cleanup)))

    vcvr
  }

  private def make_veo_bigint_vector(proc: veo_proc_handle, tsVector: TimeStampMicroTZVector)(
    implicit cleanup: Cleanup
  ): nullable_bigint_vector = {
    val keyName = "timestamp_" + tsVector.getName + "_" + tsVector.getDataBuffer.capacity()

    logger.debug(s"Copying Buffer to VE for $keyName")

    val vcvr = new nullable_bigint_vector()
      .count(tsVector.getValueCount)
      .data(new LongPointer(copyPointerToVe(proc, new BytePointer(tsVector.getDataBuffer.nioBuffer()))(cleanup)))
      .validityBuffer(new LongPointer(copyPointerToVe(proc, new BytePointer(tsVector.getValidityBuffer.nioBuffer()))(cleanup)))

    vcvr
  }

  /** Take a vec, and rewrite the pointer to our local so we can read it */
  /** Todo deallocate from VE! unless we pass it onward */
  private def veo_read_non_null_double_vector(
    proc: veo_proc_handle,
    vec: non_null_double_vector,
    bytePointer: BytePointer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = bytePointer.getLong(0)
    val dataCount = bytePointer.getInt(8)
    if (dataCount < 1) {
      // no data, do nothing
      return
    }
    val dataSize = dataCount * 8
    val vhTargetPointer = (new BytePointer(dataSize))
    requireOk(veo.veo_read_mem(proc, vhTargetPointer, veoPtr, dataSize))
    val newVec = vec.count(dataCount)
      .data(new DoublePointer(vhTargetPointer))
    cleanup.add(veoPtr, dataSize)
  }

  def veo_read_nullable_double_vector(
    proc: veo_proc_handle,
    vec: nullable_double_vector,
    bytePointer: BytePointer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = bytePointer.getLong(0)
    val validityPtr = bytePointer.getLong(8)
    val dataCount = bytePointer.getInt(16)
    if (dataCount < 1) {
      // no data, do nothing
      return
    }
    val dataSize = dataCount * 8
    val vhTargetPointer = (new BytePointer(dataSize))
    val validityTargetPointer = (new BytePointer(dataCount))

    requireOk {
      veo.veo_read_mem(proc, vhTargetPointer, veoPtr, dataSize)
      veo.veo_read_mem(proc, validityTargetPointer, validityPtr, dataCount)
    }
    val newVec = vec.count(dataCount)
      .data(new DoublePointer(vhTargetPointer))
      .validityBuffer(new LongPointer(validityTargetPointer))

    cleanup.add(veoPtr, dataSize)
    cleanup.add(validityPtr, dataCount)
  }

  private def veo_read_nullable_int_vector(
    proc: veo_proc_handle,
    vec: nullable_int_vector,
    bytePointer: BytePointer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = bytePointer.getLong(0)
    val validityPtr = bytePointer.getLong(8)
    val dataCount = bytePointer.getInt(16)
    if (dataCount < 1) {
      // no data, do nothing
      return
    }
    val dataSize = dataCount * 8
    val vhTargetPointer = (new BytePointer(dataSize))
    val vhValidityTargetPointer = (new BytePointer(dataCount))
    requireOk {
      veo.veo_read_mem(proc, vhTargetPointer, veoPtr, dataSize)
      veo.veo_read_mem(proc, vhValidityTargetPointer, validityPtr, dataCount)

    }
    val newVec = vec.count(dataCount)
      .data(new IntPointer(vhTargetPointer))
      .validityBuffer(new LongPointer(vhValidityTargetPointer))

    cleanup.add(veoPtr, dataSize)
    cleanup.add(validityPtr, dataSize)
  }

  private def veo_read_non_null_bigint_vector(
    proc: veo_proc_handle,
    vec: non_null_bigint_vector,
    bytePointer: BytePointer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = bytePointer.getLong(0)
    val dataCount = bytePointer.getInt(8)
    if (dataCount < 1) {
      // no data, do nothing
      return
    }
    val dataSize = dataCount * 8
    val vhTargetPointer = (new BytePointer(dataSize))
    requireOk(veo.veo_read_mem(proc, vhTargetPointer, veoPtr, dataSize))
    val newVec = vec.count(dataCount)
      .data(new LongPointer(vhTargetPointer))
    cleanup.add(veoPtr, dataSize)
  }

  private def veo_read_nullable_bigint_vector(
    proc: veo_proc_handle,
    vec: nullable_bigint_vector,
    bytePointer: BytePointer
  )(implicit cleanup: Cleanup): Unit = {
    val veoPtr = bytePointer.getLong(0)
    val validityPtr = bytePointer.getLong(8)
    val dataCount = bytePointer.getInt(16)
    val dataSize = dataCount * 8

    if (dataCount < 1) {
      // no data, do nothing
      return
    }

    val vhTargetPointer = (new BytePointer(dataSize))
    val validityTargetPointer = (new BytePointer(dataCount))

    requireOk {
      veo.veo_read_mem(proc, vhTargetPointer, veoPtr, dataSize)
    }

    requireOk {
      veo.veo_read_mem(proc, validityTargetPointer, validityPtr, dataCount)
    }
    val newVec = vec.count(dataCount)
      .data(new LongPointer(vhTargetPointer))
      .validityBuffer(new LongPointer(validityTargetPointer))

    cleanup.add(veoPtr, dataSize)
    cleanup.add(validityPtr, dataSize)
  }

  private def veo_read_nullable_varchar_vector(
    proc: veo_proc_handle,
    vec: nullable_varchar_vector,
    bytePointer: BytePointer
  )(implicit cleanup: Cleanup): Unit = {

    /** Get data size */
    val dataSize = bytePointer.getInt(32)
    val dataCount = bytePointer.getInt(36)

    val newVec = vec.dataSize(dataSize)
      .count(dataCount)

    if (dataCount < 1) {
      // no data, do nothing
      return
    }

    /** Transfer the data */
    val dataPtr = bytePointer.getLong(0)
    requirePositive(dataPtr)
    val vhTargetDataPointer = (new BytePointer(dataSize))
    requireOk {
      veo.veo_read_mem(proc, vhTargetDataPointer, dataPtr, dataSize)
    }
    val newVec2 = newVec.data(new IntPointer(vhTargetDataPointer))
    cleanup.add(dataPtr, dataSize)

    /** Transfer the offsets */
    val offsetsPtr = bytePointer.getLong(8)
    requirePositive(offsetsPtr)
    val vhTargetOffsetsPointer = (new BytePointer((dataCount + 1) * 4))
    requireOk {
      veo.veo_read_mem(proc, vhTargetOffsetsPointer, offsetsPtr, (dataCount + 1) * 4)
    }
    val newVec3 = newVec2.offsets(new IntPointer(vhTargetOffsetsPointer))
    cleanup.add(offsetsPtr, (dataCount + 1) * 4)

    /** Transfer the validity buffer */
    val validityPtr = bytePointer.getLong(16)
    requirePositive(validityPtr)
    val vhValidityPointer = (new BytePointer(Math.ceil(vec.count / 64.0).toInt * 8))
    requireOk {
      veo.veo_read_mem(proc, vhValidityPointer, validityPtr, Math.ceil(vec.count / 64.0).toInt * 8)
    }
    val newVec4 = newVec3.validityBuffer(new LongPointer(vhValidityPointer))
    cleanup.add(validityPtr, Math.ceil(vec.count / 64.0).toInt * 8)
  }

  def stringToBytePointer(str_buf: non_null_c_bounded_string): BytePointer = {
    val v_bb = str_buf.asByteBuffer
    v_bb.putLong(0, str_buf.data.address)
    v_bb.putInt(8, str_buf.length)
    new BytePointer(v_bb)
  }

  def nullableDoubleVectorToBytePointer(double_vector: nullable_double_vector): BytePointer = {
    val v_bb = double_vector.asByteBuffer
    v_bb.putLong(0, double_vector.data.address)
    v_bb.putLong(8, double_vector.validityBuffer.address)
    v_bb.putInt(16, double_vector.count)
    new BytePointer(v_bb)
  }

  def nullableBigintVectorToBytePointer(bigint_vector: nullable_bigint_vector): BytePointer = {
    val v_bb = bigint_vector.asByteBuffer
    v_bb.putLong(0, bigint_vector.data.address)
    v_bb.putLong(8, bigint_vector.validityBuffer.address)
    v_bb.putInt(16, bigint_vector.count)
    new BytePointer(v_bb)
  }

  def nullableIntVectorToBytePointer(int_vector: nullable_int_vector): BytePointer = {
    val v_bb = int_vector.asByteBuffer
    v_bb.putLong(0, int_vector.data.address)
    v_bb.putLong(8, int_vector.validityBuffer.address)
    v_bb.putInt(16, int_vector.count)
    new BytePointer(v_bb)
  }

  def nonNullDoubleVectorToBytePointer(double_vector: non_null_double_vector): BytePointer = {
    val v_bb = double_vector.asByteBuffer
    v_bb.putLong(0, double_vector.data.address)
    v_bb.putInt(8, double_vector.count)
    new BytePointer(v_bb)
  }

  def nonNullInt2VectorToBytePointer(int_vector: non_null_int2_vector): BytePointer = {
    val v_bb = int_vector.asByteBuffer
    v_bb.putLong(0, int_vector.data.address)
    v_bb.putInt(8, int_vector.count)
    new BytePointer(v_bb)
  }

  def nonNullBigIntVectorToBytePointer(bigint_vector: non_null_bigint_vector): BytePointer = {
    val v_bb = bigint_vector.asByteBuffer
    v_bb.putLong(0, bigint_vector.data.address)
    v_bb.putInt(8, bigint_vector.count)
    new BytePointer(v_bb)
  }

  def nullableVarCharVectorVectorToBytePointer(
    varchar_vector: nullable_varchar_vector
  ): BytePointer = {
    val v_bb = varchar_vector.asByteBuffer
    v_bb.putLong(0, varchar_vector.data.address)
    v_bb.putLong(8, varchar_vector.offsets.address)
    v_bb.putLong(16, varchar_vector.lengths.address)
    v_bb.putLong(24, varchar_vector.validityBuffer.address)
    v_bb.putInt(32, varchar_vector.dataSize)
    v_bb.putInt(36, varchar_vector.count)
    new BytePointer(v_bb)
  } 
}
