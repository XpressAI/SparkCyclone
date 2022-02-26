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
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.BytePointer

object VeArrowTransfers extends LazyLogging {

  def stringToBytePointer(str_buf: non_null_c_bounded_string): BytePointer = {
    val v_bb = str_buf.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, str_buf.data)
    v_bb.putInt(8, str_buf.length)
    new BytePointer(v_bb)
  }

  def nullableDoubleVectorToBytePointer(double_vector: nullable_double_vector): BytePointer = {
    val v_bb = double_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, double_vector.data)
    v_bb.putLong(8, double_vector.validityBuffer)
    v_bb.putInt(16, double_vector.count)
    new BytePointer(v_bb)
  }

  def nullableBigintVectorToBytePointer(bigint_vector: nullable_bigint_vector): BytePointer = {
    val v_bb = bigint_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, bigint_vector.data)
    v_bb.putLong(8, bigint_vector.validityBuffer)
    v_bb.putInt(16, bigint_vector.count)
    new BytePointer(v_bb)
  }

  def nullableIntVectorToBytePointer(int_vector: nullable_int_vector): BytePointer = {
    val v_bb = int_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, int_vector.data)
    v_bb.putLong(8, int_vector.validityBuffer)
    v_bb.putInt(16, int_vector.count)
    new BytePointer(v_bb)
  }

  def nullableShortVectorToBytePointer(short_vector: nullable_short_vector): BytePointer = {
    val v_bb = short_vector.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, short_vector.data)
    v_bb.putLong(8, short_vector.validityBuffer)
    v_bb.putInt(16, short_vector.count)
    new BytePointer(v_bb)
  }

  def nonNullDoubleVectorToBytePointer(double_vector: non_null_double_vector): BytePointer = {
    val v_bb = double_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, double_vector.data)
    v_bb.putInt(8, double_vector.count)
    new BytePointer(v_bb)
  }

  def nonNullInt2VectorToBytePointer(int_vector: non_null_int2_vector): BytePointer = {
    val v_bb = int_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, int_vector.data)
    v_bb.putInt(8, int_vector.count)
    new BytePointer(v_bb)
  }

  def nonNullBigIntVectorToBytePointer(bigint_vector: non_null_bigint_vector): BytePointer = {
    val v_bb = bigint_vector.getPointer.getByteBuffer(0, 12)
    v_bb.putLong(0, bigint_vector.data)
    v_bb.putInt(8, bigint_vector.count)
    new BytePointer(v_bb)
  }

  def nullableVarCharVectorVectorToBytePointer(
    varchar_vector: nullable_varchar_vector
  ): BytePointer = {
    val v_bb = varchar_vector.getPointer.getByteBuffer(0, (8 * 4) + (4 * 2))
    v_bb.putLong(0, varchar_vector.data)
    v_bb.putLong(8, varchar_vector.offsets)
    v_bb.putLong(16, varchar_vector.lengths)
    v_bb.putLong(24, varchar_vector.validityBuffer)
    v_bb.putInt(32, varchar_vector.dataSize)
    v_bb.putInt(36, varchar_vector.count)
    new BytePointer(v_bb)
  }
}
