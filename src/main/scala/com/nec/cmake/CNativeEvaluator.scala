package com.nec.cmake
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.spark.planning.NativeEvaluator

object CNativeEvaluator extends NativeEvaluator {
  override def forCode(code: String): ArrowNativeInterfaceNumeric = {
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, code)
        .mkString("\n\n")
    )
    new CArrowNativeInterfaceNumeric(cLib.toAbsolutePath.toString)
  }
}
