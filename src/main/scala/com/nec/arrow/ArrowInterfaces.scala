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

import org.apache.arrow.vector._
import java.nio.{ByteBuffer, ByteOrder}

object ArrowInterfaces {

  def intCharsFromVarcharVector(buf: VarCharVector): ByteBuffer = {
    val ret = ByteBuffer
      .allocateDirect(buf.getDataBuffer.readableBytes().toInt * 4)
      .order(ByteOrder.LITTLE_ENDIAN)
    val out = ret.asIntBuffer()

    for (i <- 0 until buf.getValueCount) {
      val ints = buf.getObject(i).toString.getBytes("UTF-32LE")
      val intBuf = ByteBuffer.wrap(ints).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer()
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

}
