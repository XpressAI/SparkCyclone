package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.cmake.functions.ParseCSVSpec.RichBigIntVector
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import org.apache.arrow.vector.BigIntVector
import org.scalatest.freespec.AnyFreeSpec

final class LongBigIntRetrieveSpec extends AnyFreeSpec {
  "It works" in {
    WithTestAllocator { implicit allocator =>
      val outVector_0 = new BigIntVector("output_0", allocator)

      val cLib = CMakeBuilder.buildC(
        List(
          TransferDefinitionsSourceCode,
          "\n\n",
          CodeLines
            .from(
              """ extern "C" long x(nullable_bigint_vector *v) { """,
              "v->count = 1;",
              "v->data = (int64_t *)malloc(8);",
              "v->validityBuffer = (unsigned char *) malloc(8); ",
              "v->data[0] = 123;",
              " set_validity(v->validityBuffer, 0, 1); ",
              "return 0;",
              """ }"""
            )
            .cCode
        )
          .mkString("\n\n")
      )

      val nativeInterface = new CArrowNativeInterface(cLib.toString)
      val res =
        try {
          nativeInterface.callFunctionWrapped("x", List(NativeArgument.output(outVector_0)))
          outVector_0.toList
        } finally { outVector_0.close() }

      assert(res == List[Long](123))
    }
  }

}
