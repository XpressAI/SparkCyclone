package com.nec.cmake

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.WithTestAllocator
import com.nec.native.NativeEvaluator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.planning.Tracer
import org.scalatest.freespec.AnyFreeSpec

class TracerTest extends AnyFreeSpec {
  lazy val evaluator: NativeEvaluator = NativeEvaluator.CNativeEvaluator
  "We can trace" in {
    val functionName = "test"
    val ani = evaluator.forCode(code =
      List(
        Tracer.DefineTracer.cCode,
        "\n\n",
        CFunction(inputs = List(Tracer.TracerVector), outputs = Nil, body = CodeLines.debugHere)
          .toCodeLinesNoHeader(functionName)
          .cCode
      )
        .mkString("\n\n")
    )
    WithTestAllocator { implicit allocator =>
      val inVec = Tracer.Launched("launchId").map("mappingId")
      val vec = inVec.createVector()
      try {
        ani.callFunctionWrapped(functionName, List(NativeArgument.input(vec)))
      } finally vec.close()
    }
  }
}
