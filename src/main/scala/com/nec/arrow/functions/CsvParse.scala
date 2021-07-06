package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.ByteBufferWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.StringWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.{Float8VectorWrapper, StringWrapper}
import org.apache.arrow.vector.Float8Vector

import java.nio.ByteBuffer

object CsvParse {

  val CsvParseCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("csv.cpp"))
    try source.mkString
    finally source.close()
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
      outputArguments = List(
        None,
        Some(Float8VectorWrapper(a)),
        Some(Float8VectorWrapper(b)),
        Some(Float8VectorWrapper(c))
      )
    )
  }
}
