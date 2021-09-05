package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.spark.agile.ExprEvaluation2
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ExpressionEvaluationSpec extends AnyFreeSpec {
  "We can transform a column" in {

  }
  "We can filter a column" in {
    val input: Seq[Double] = Seq(90.0, 1.0, 2, 19, 14)
    val generatedSource = ExprEvaluation2.filterDouble

    info(generatedSource.cCode)
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitionsSourceCode, "\n\n", generatedSource.cCode)
        .mkString("\n\n")
    )
    withDirectFloat8Vector(input) { vector =>
      WithTestAllocator { alloc =>
        val outVector = new Float8Vector("value", alloc)
        try {
          val nativeInterface = new CArrowNativeInterface(cLib.toString)
          nativeInterface.callFunctionWrapped(
            "filter_f",
            List(NativeArgument.input(vector), NativeArgument.output(outVector))
          )
          val outData = (0 until outVector.getValueCount).map(idx => outVector.get(idx)).toList
          assert(outData == List[Double](90, 19))
        } finally outVector.close()
      }
    }
  }
  "We can aggregate" in {

  }
  "We can join" in {

  }
}
