package com.nec.cmake
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.spark.planning.CEvaluationPlan.NativeEvaluator
import com.nec.arrow.TransferDefinitions

object CNativeEvaluator extends NativeEvaluator {
  override def forCode(code: String): ArrowNativeInterfaceNumeric = {
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, code)
        .mkString("\n\n")
    )
    new CArrowNativeInterfaceNumeric(cLib.toAbsolutePath.toString)
  }
}
