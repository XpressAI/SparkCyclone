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
package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.ByteBufferInputWrapper
import com.nec.arrow.ArrowNativeInterface.NativeArgument.VectorInputNativeArgument.InputVectorWrapper.StringInputWrapper
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector

import java.nio.ByteBuffer

object CsvParse {

  val CsvParseCode: String = {
    val sources = Seq(scala.io.Source.fromInputStream(getClass.getResourceAsStream("csv.cpp")))
    try sources.map(_.mkString("")).mkString("\n")
    finally sources.map(_.close())
  }

  def runOn(nativeInterface: ArrowNativeInterface)(
    input: Either[(ByteBuffer, Int), String],
    a: Float8Vector,
    b: Float8Vector,
    c: Float8Vector
  ): Unit = {
    nativeInterface.callFunctionWrapped(
      "parse_csv",
      List(
        input.fold(
          { case (bb, i) =>
            NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(bb, i))
          },
          str => NativeArgument.VectorInputNativeArgument(StringInputWrapper(str))
        ),
        NativeArgument.output(a),
        NativeArgument.output(b),
        NativeArgument.output(c)
      )
    )
  }

  def runOn2(
    nativeInterface: ArrowNativeInterface
  )(input: Either[(ByteBuffer, Int), String], a: Float8Vector, b: Float8Vector): Unit = {
    nativeInterface.callFunctionWrapped(
      "parse_csv_2",
      List(
        input.fold(
          { case (bb, i) =>
            NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(bb, i))
          },
          str => NativeArgument.VectorInputNativeArgument(StringInputWrapper(str))
        ),
        NativeArgument.output(a),
        NativeArgument.output(b)
      )
    )
  }

  def double1str2int3long4(nativeInterface: ArrowNativeInterface)(
    input: Either[(ByteBuffer, Int), String],
    a: Float8Vector,
    b: VarCharVector,
    c: IntVector,
    d: BigIntVector
  ): Unit = {
    nativeInterface.callFunctionWrapped(
      "parse_csv_double1_str2_int3_long4",
      List(
        input.fold(
          { case (bb, i) =>
            NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(bb, i))
          },
          str => NativeArgument.VectorInputNativeArgument(StringInputWrapper(str))
        ),
        NativeArgument.output(a),
        NativeArgument.output(b),
        NativeArgument.output(c),
        NativeArgument.output(d)
      )
    )
  }
}
