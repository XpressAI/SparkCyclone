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
          { case (bb, i) => NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(bb, i)) },
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
          { case (bb, i) => NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(bb, i)) },
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
          { case (bb, i) => NativeArgument.VectorInputNativeArgument(ByteBufferInputWrapper(bb, i)) },
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
