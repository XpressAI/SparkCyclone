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

import com.nec.arrow.ArrowTransferStructures._
import org.apache.arrow.vector._
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.bytedeco.javacpp.BytePointer
import sun.misc.Unsafe

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
    vc.data = float8Vector.getDataBufferAddress()

    vc.count = float8Vector.getValueCount
    vc
  }

  def c_nullable_double_vector(float8Vector: Float8Vector): nullable_double_vector = {
    val vc = new nullable_double_vector()
    vc.data = float8Vector.getDataBufferAddress()
    vc.validityBuffer = float8Vector.getValidityBufferAddress
    vc.count = float8Vector.getValueCount
    vc
  }

  def c_nullable_varchar_vector(varCharVector: VarCharVector): nullable_varchar_vector = {
    val vc = new nullable_varchar_vector()
    vc.data = varCharVector.getDataBufferAddress()
    vc.offsets = varCharVector.getOffsetBufferAddress()
    vc.validityBuffer =
      varCharVector.getValidityBufferAddress()
    vc.count = varCharVector.getValueCount
    vc.dataSize = varCharVector.sizeOfValueBuffer()
    vc
  }

  def c_bounded_string(string: String): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
    vc.data =  (new BytePointer(string.length))
      .put(string.getBytes(): _*)
      .address()
    vc.length = string.length
    vc
  }

  def c_bounded_data(bytePointer: BytePointer, bufSize: Int): non_null_c_bounded_string = {
    val vc = new non_null_c_bounded_string()
    vc.data = bytePointer.address()
    vc.length = bufSize
    vc
  }

  def c_int2_vector(intVector: IntVector): non_null_int2_vector = {
    val vc = new non_null_int2_vector()
    vc.data = intVector.getDataBufferAddress()
    vc.count = intVector.getValueCount
    vc
  }

  def c_nullable_int_vector(intVector: IntVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    vc.data = intVector.getDataBufferAddress()
    vc.validityBuffer = intVector.getValidityBufferAddress()
    vc.count = intVector.getValueCount
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
    vc.data = intVector.getDataBufferAddress()
    vc.validityBuffer = bitVector.getValidityBufferAddress()
    vc.count = bitVector.getValueCount
    vc
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
    vc.data = intVector.getDataBufferAddress()
    vc.validityBuffer =
      smallIntVector.getValidityBufferAddress()
    vc.count = smallIntVector.getValueCount
    vc
  }

  def c_nullable_bigint_vector(tzVector: TimeStampMicroTZVector): nullable_bigint_vector = {
    val vc = new nullable_bigint_vector()
    vc.data = tzVector.getDataBufferAddress()
    vc.validityBuffer = tzVector.getValidityBufferAddress()
    vc.count = tzVector.getValueCount
    vc
  }

  def c_nullable_bigint_vector(bigIntVector: BigIntVector): nullable_bigint_vector = {
    val vc = new nullable_bigint_vector()
    vc.data = bigIntVector.getDataBufferAddress()
    vc.validityBuffer =
      bigIntVector.getValidityBufferAddress()
    vc.count = bigIntVector.getValueCount
    vc
  }

  def c_nullable_date_vector(dateDayVector: DateDayVector): nullable_int_vector = {
    val vc = new nullable_int_vector()
    vc.data = dateDayVector.getDataBufferAddress()
    vc.validityBuffer =
      dateDayVector.getValidityBufferAddress()
    vc.count = dateDayVector.getValueCount
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
    getUnsafe.copyMemory(input.data, intVector.getDataBufferAddress, input.count * 4)
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
    getUnsafe.copyMemory(input.data, bigintVector.getDataBufferAddress, input.dataSize())
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
      input.validityBuffer,
      bigintVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data, bigintVector.getDataBufferAddress, input.dataSize())
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
    getUnsafe.copyMemory(input.data, float8Vector.getDataBufferAddress, input.dataSize())
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
      input.validityBuffer,
      float8Vector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data, float8Vector.getDataBufferAddress, input.dataSize())
  }

  def non_null_int2_vector_to_IntVector(input: non_null_int2_vector, intVector: IntVector): Unit = {
    if (input.count < 1) {
      return
    }
    intVector.setValueCount(input.count)
    (0 until input.count).foreach(i => BitVectorHelper.setBit(intVector.getValidityBuffer, i))
    getUnsafe.copyMemory(input.data, intVector.getDataBufferAddress, input.size())
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
      input.validityBuffer,
      intVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data, intVector.getDataBufferAddress, input.dataSize())
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
    varCharVector.allocateNew(input.dataSize.toLong, input.count)
    varCharVector.setValueCount(input.count)
    getUnsafe.copyMemory(
      input.validityBuffer,
      varCharVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data, varCharVector.getDataBufferAddress, input.dataSize.toLong)
    getUnsafe.copyMemory(input.offsets, varCharVector.getOffsetBufferAddress, 4 * (input.count + 1))
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
      input.validityBuffer,
      timeStampVector.getValidityBufferAddress,
      Math.ceil(input.count / 64.0).toInt * 8
    )
    getUnsafe.copyMemory(input.data, timeStampVector.getDataBufferAddress, input.dataSize())
  }
}
