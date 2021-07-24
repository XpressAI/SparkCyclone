package com.nec.arrow.functions

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.BigIntVectorWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.ByteBufferWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.IntVectorWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.VarCharVectorWrapper
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper.{StringWrapper, Float8VectorWrapper}
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector

import java.nio.ByteBuffer

object CsvParse {

  val CsvParseCode: String = {
    val sources = Seq(
      scala.io.Source.fromInputStream(getClass.getResourceAsStream("csv.cpp"))
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
      outputArguments = List(
        None,
        Some(Float8VectorWrapper(a)),
        Some(Float8VectorWrapper(b)),
        Some(Float8VectorWrapper(c))
      )
    )
  }

  def runOn2(nativeInterface: ArrowNativeInterfaceNumeric)(
    input: Either[(ByteBuffer, Int), String],
    a: Float8Vector,
    b: Float8Vector
  ): Unit = {
    nativeInterface.callFunction(
      name = "parse_csv_2",
      inputArguments = List(
        Some(input.fold(Function.tupled(ByteBufferWrapper.apply), StringWrapper)),
        None,
        None
      ),
      outputArguments = List(
        None,
        Some(Float8VectorWrapper(a)),
        Some(Float8VectorWrapper(b))
      )
    )
  }

  def double1str2int3long4(nativeInterface: ArrowNativeInterfaceNumeric)(
    input: Either[(ByteBuffer, Int), String],
    a: Float8Vector,
    b: VarCharVector,
    c: IntVector,
    d: BigIntVector
  ): Unit = {
    nativeInterface.callFunction(
      name = "parse_csv_double1_str2_int3_long4",
      inputArguments = List(
        Some(input.fold(Function.tupled(ByteBufferWrapper.apply), StringWrapper)),
        None,
        None,
        None,
        None
      ),
      outputArguments = List(
        None,
        Some(Float8VectorWrapper(a)),
        Some(VarCharVectorWrapper(b)),
        Some(IntVectorWrapper(c)),
        Some(BigIntVectorWrapper(d))
      )
    )
  }
}
