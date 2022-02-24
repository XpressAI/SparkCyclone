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

import com.nec.arrow.TransferDefinitions._
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.bytedeco.javacpp.BytePointer
import org.bytedeco.javacpp.DoublePointer
import org.bytedeco.javacpp.LongPointer
import org.bytedeco.javacpp.IntPointer
import org.bytedeco.javacpp.ShortPointer
import sun.misc.Unsafe
import sun.nio.ch.DirectBuffer

import java.nio.{ByteBuffer, ByteOrder}

object ArrowInterfaces {

  def non_null_int_vector_to_IntVector(input: non_null_int_vector, output: IntVector): Unit = {
    non_null_int_vector_to_intVector(input, output)
  }

  def non_null_bigint_vector_to_bigIntVector(
    input: non_null_bigint_vector,
    output: BigIntVector
  ): Unit = {
    non_null_bigint_vector_to_bigintVector(input, output)
  }

  def c_double_vector(float8Vector: Float8Vector): non_null_double_vector = {
    val vc = new non_null_double_vector()
      .data(new DoublePointer(float8Vector.getDataBuffer().nioBuffer.asDoubleBuffer))
      .count(float8Vector.getValueCount)
    vc
  }

  def c_nullable_double_vector(float8Vector: Float8Vector): nullable_double_vector = {
    val vc = new nullable_double_vector()
      .data(new DoublePointer(float8Vector.getDataBuffer().nioBuffer.asDoubleBuffer))
      .validityBuffer(new LongPointer(float8Vector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(float8Vector.getValueCount)
    vc
  }

  def intCharsFromVarcharVector(buf: VarCharVector): ByteBuffer = {
    val ret = ByteBuffer
      .allocateDirect(buf.getDataBuffer.readableBytes().toInt * 4)
      .order(ByteOrder.LITTLE_ENDIAN)
    val out = ret.asIntBuffer()

    for (i <- 0 until buf.getValueCount) {
      val ints = buf.getObject(i).toString.getBytes("UTF-32LE")
      val intBuf = ByteBuffer.wrap(ints).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
      out.put(intBuf)
    }
    ret
  }

  def lengthsFromVarcharVector(buf: VarCharVector): ByteBuffer = {
    val ret = ByteBuffer.allocateDirect(buf.getValueCount * 4).order(ByteOrder.LITTLE_ENDIAN)
    val lengths = ret.asIntBuffer()
    for (i <- 0 until buf.getValueCount) {
      val len = buf.getEndOffset(i) - buf.getStartOffset(i)
      lengths.put(len)
    }
    ret
  }

  def startsFromVarcharVector(buf: VarCharVector): ByteBuffer = {
    val ret = ByteBuffer.allocateDirect(buf.getValueCount * 4).order(ByteOrder.LITTLE_ENDIAN)
    val starts = ret.asIntBuffer()
    for (i <- 0 until buf.getValueCount) {
      val len = buf.getStartOffset(i)

      starts.put(len)
    }
    ret
  }

  def c_nullable_varchar_vector(varCharVector: VarCharVector): nullable_varchar_vector = {
    val vc = new nullable_varchar_vector()
      .data(new IntPointer(intCharsFromVarcharVector(varCharVector).asIntBuffer))
      .offsets(new IntPointer(startsFromVarcharVector(varCharVector).asIntBuffer))
      .lengths(new IntPointer(lengthsFromVarcharVector(varCharVector).asIntBuffer))
      .validityBuffer(new LongPointer(varCharVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(varCharVector.getValueCount)
      .dataSize(varCharVector.sizeOfValueBuffer())
    vc
  }

  def c_bounded_string(string: String): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
      .data((new BytePointer(string.length))
      .put(string.getBytes("UTF-32LE"): _*))
      .length(string.length)
    vc
  }

  def c_bounded_data(bytePointer: BytePointer, bufSize: Int): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
      .data(bytePointer)
      .length(bufSize)
    vc
  }

  def c_int2_vector(intVector: IntVector): non_null_int2_vector = {
    val vc = new non_null_int2_vector()
      .data(new ShortPointer(intVector.getDataBuffer().nioBuffer.asShortBuffer))
      .count(intVector.getValueCount)
    vc
  }

  def c_nullable_int_vector(intVector: IntVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
      .data(new IntPointer(intVector.getDataBuffer().nioBuffer.asIntBuffer))
      .validityBuffer(new LongPointer(intVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(intVector.getValueCount)
    vc
  }

  def c_nullable_bit_vector(bitVector: BitVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(bitVector.getValueCount)

    (0 until bitVector.getValueCount).foreach {
      case idx if (!bitVector.isNull(idx)) => intVector.set(idx, bitVector.get(idx))
      case idx                             => intVector.setNull(idx)
    }
    val newVc = vc.data(new IntPointer(intVector.getDataBuffer().nioBuffer.asIntBuffer))
      .validityBuffer(new LongPointer(bitVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(bitVector.getValueCount)
    newVc
  }

  def c_nullable_int_vector(smallIntVector: SmallIntVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    val intVector = new IntVector("name", ArrowUtilsExposed.rootAllocator)
    intVector.setValueCount(smallIntVector.getValueCount)

    (0 until smallIntVector.getValueCount)
      .foreach {
        case idx if (!smallIntVector.isNull(idx)) =>
          intVector.set(idx, smallIntVector.get(idx).toInt)
        case idx => intVector.setNull(idx)
      }
    val newVc = vc.data(new IntPointer(intVector.getDataBuffer().nioBuffer.asIntBuffer))
      .validityBuffer(
      new LongPointer(smallIntVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(smallIntVector.getValueCount)
    newVc
  }

  def c_nullable_bigint_vector(tzVector: TimeStampMicroTZVector): nullable_bigint_vector = {
    val vc = new nullable_bigint_vector()
      .data(new LongPointer(tzVector.getDataBuffer().nioBuffer.asLongBuffer))
      .validityBuffer(new LongPointer(tzVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(tzVector.getValueCount)
    vc
  }

  def c_nullable_bigint_vector(bigIntVector: BigIntVector): nullable_bigint_vector = {
    val vc = new nullable_bigint_vector()
      .data(new LongPointer(bigIntVector.getDataBuffer().nioBuffer.asLongBuffer))
      .validityBuffer(new LongPointer(bigIntVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(bigIntVector.getValueCount)
    vc
  }

  def c_nullable_date_vector(dateDayVector: DateDayVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
      .data(new IntPointer(dateDayVector.getDataBuffer().nioBuffer.asIntBuffer))
      .validityBuffer(new LongPointer(dateDayVector.getValidityBuffer().nioBuffer.asLongBuffer))
      .count(dateDayVector.getValueCount)
    vc
  }

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def non_null_int_vector_to_intVector(input: non_null_int_vector, intVector: IntVector): Unit = {
    if (input.count < 1) {
      return
    }
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(intVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data.address, intVector.getDataBufferAddress, input.count * 4)
  }

  def non_null_bigint_vector_to_bigintVector(
    input: non_null_bigint_vector,
    bigintVector: BigIntVector
  ): Unit = {
    if (input.count < 1) {
      return
    }
    bigintVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(bigintVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data.address, bigintVector.getDataBufferAddress, input.dataSize())
  }

  def nullable_bigint_vector_to_BigIntVector(
    input: nullable_bigint_vector,
    bigintVector: BigIntVector
  ): Unit = {
    if (input.count < 1) {
      return
    }
    bigintVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer.address,
      bigintVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data.address, bigintVector.getDataBufferAddress, input.dataSize())
  }

  def non_null_double_vector_to_float8Vector(
    input: non_null_double_vector,
    float8Vector: Float8Vector
  ): Unit = {
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    if (input.count < 1) {
      return
    }
    float8Vector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(float8Vector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data.address, float8Vector.getDataBufferAddress, input.dataSize())
  }

  def nullable_double_vector_to_float8Vector(
    input: nullable_double_vector,
    float8Vector: Float8Vector
  ): Unit = {
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    if (input.count < 1) {
      return
    }
    float8Vector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer.address,
      float8Vector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data.address, float8Vector.getDataBufferAddress, input.dataSize())
  }

  def non_null_int2_vector_to_IntVector(input: non_null_int2_vector, intVector: IntVector): Unit = {
    if (input.count < 1) {
      return
    }
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(intVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data.address, intVector.getDataBufferAddress, input.size())
  }

  def nullable_int_vector_to_IntVector(input: nullable_int_vector, intVector: IntVector): Unit = {
    if (input.count < 1) {
      return
    }
    if (input.count == 0xffffffff) {
      sys.error(s"Returned count was infinite; input ${input}")
    }
    intVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer.address,
      intVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data.address, intVector.getDataBufferAddress, input.dataSize())
  }

  def nullable_int_vector_to_SmallIntVector(
    input: nullable_int_vector,
    smallIntVector: SmallIntVector
  ): Unit = {
    if (input.count < 1) {
      return
    }
    val intVector = new IntVector("temp", ArrowUtilsExposed.rootAllocator)
    nullable_int_vector_to_IntVector(input, intVector)
    smallIntVector.setValueCount(intVector.getValueCount)
    (0 until intVector.getValueCount).foreach {
      case idx if (intVector.isNull(idx)) => smallIntVector.setNull(idx)
      case idx                            => smallIntVector.set(idx, intVector.get(idx).toShort)
    }
    intVector.clear()
    intVector.close()
  }

  def nullable_varchar_vector_to_VarCharVector(
    input: nullable_varchar_vector,
    varCharVector: VarCharVector
  ): Unit = {
    if (input.count < 1) {
      return
    }
    val buf = ByteBuffer.allocateDirect(input.count * 4).order(ByteOrder.LITTLE_ENDIAN)
    getUnsafe.copyMemory(input.lengths.address, buf.asInstanceOf[DirectBuffer].address(), input.count * 4)
    val lengths = buf.asIntBuffer()
    var sum = 0;
    for (i <- 0 until input.count) {
      val len = lengths.get(i)
      sum += len
    }

    varCharVector.allocateNew(sum, input.count)
    varCharVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer.address,
      varCharVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    val dataBuf = ByteBuffer.allocateDirect(input.dataSize * 4).order(ByteOrder.LITTLE_ENDIAN)
    getUnsafe.copyMemory(
      input.data.address,
      dataBuf.asInstanceOf[DirectBuffer].address(),
      input.dataSize * 4
    )

    val dataBufArray =
      ByteBuffer.wrap(new Array[Byte](dataBuf.capacity())).order(ByteOrder.LITTLE_ENDIAN)
    dataBufArray.put(dataBuf)

    for (i <- 0 until input.count) {
      val start = getUnsafe.getInt(input.offsets.get + (i * 4)) * 4
      val length = getUnsafe.getInt(input.lengths.get + (i * 4)) * 4
      val str = new String(dataBufArray.array(), start, length, "UTF-32LE")
      val utf8bytes = str.getBytes

      varCharVector.set(i, utf8bytes)
    }
  }

  def nullable_int_vector_to_BitVector(input: nullable_int_vector, bitVector: BitVector): Unit = {
    if (input.count < 1) {
      return
    }
    val intVector = new IntVector("temp", ArrowUtilsExposed.rootAllocator)
    nullable_int_vector_to_IntVector(input, intVector)
    bitVector.setValueCount(intVector.getValueCount)
    (0 until intVector.getValueCount).foreach {
      case idx if (intVector.isNull(idx)) =>
        bitVector.setNull(idx)
      case idx =>
        bitVector.set(idx, intVector.get(idx))
    }
    intVector.clear()
    intVector.close()
  }

  def nullable_bigint_vector_to_TimeStampVector(
    input: nullable_bigint_vector,
    timeStampVector: TimeStampMicroTZVector
  ): Unit = {
    if (input.count < 1) {
      return
    }
    timeStampVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer.address,
      timeStampVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data.address, timeStampVector.getDataBufferAddress, input.dataSize())
  }
}
