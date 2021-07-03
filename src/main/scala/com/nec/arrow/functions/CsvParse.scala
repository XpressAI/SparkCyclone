package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.StringWrapper
import org.apache.arrow.vector.Float8Vector

object CsvParse {

  val CsvParseCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("csv.cpp"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterfaceNumeric
  )(input: String, a: Float8Vector, b: Float8Vector, c: Float8Vector): Unit = {
    nativeInterface.callFunction(
      name = "parse_csv",
      inputArguments = List(Some(StringWrapper(input)), None, None, None),
      outputArguments = List(None, Some(a), Some(b), Some(c))
    )
  }
}
