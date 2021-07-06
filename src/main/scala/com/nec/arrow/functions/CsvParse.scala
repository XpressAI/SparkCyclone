package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.ByteBufferWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.StringWrapper
import org.apache.arrow.vector.Float8Vector

import java.nio.ByteBuffer

object CsvParse {

  val CsvParseCode: String = {
    val sources = Seq(
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("csv.cpp")),
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("cpp/frovedis/text/char_int_conv.cc")),
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("cpp/frovedis/core/utility.cc")),
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("cpp/frovedis/text/find.cc")),
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("cpp/frovedis/text/words.cc")),
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("cpp/frovedis/text/parsefloat.cc")),
    )
    try sources.map(_.mkString("")).mkString("\n")
    finally sources.map(_.close())
  }

  def runOn(nativeInterface: ArrowNativeInterfaceNumeric)(
    input: Either[(ByteBuffer, Int), String],
    a: Float8Vector,
    b: Float8Vector,
    c: Float8Vector
  ): Unit = {
    nativeInterface.callFunction(
      name = "parse_csv",
      inputArguments = List(
        Some(input.fold(Function.tupled(ByteBufferWrapper.apply), StringWrapper)),
        None,
        None,
        None
      ),
      outputArguments = List(None, Some(a), Some(b), Some(c))
    )
  }
}
