package io.sparkcyclone.native

import io.sparkcyclone.spark.codegen.core.{CFunction2, CodeLines}
import io.sparkcyclone.spark.codegen.core.CFunction2.CFunctionArgument
import scala.util.Random

object NativeFunctionSamples {
  def sampleFunction: NativeFunction = {
    new NativeFunction {
      lazy val hashId: Int = {
        Random.nextInt
      }

      lazy val id: Int = {
        Random.nextInt(1000)
      }

      lazy val primary: CFunction2 = {
        CFunction2(
          s"func1_${id}",
          Seq(CFunctionArgument.Raw("double input")),
          // Random code to call the secondary functions
          CodeLines.from(s"return input * func2_${id}(input) + func3_${id}(input / 3.0);")
        )
      }

      lazy val secondary: Seq[CFunction2] = {
        Seq(
          CFunction2(
            s"func2_${id}",
            Seq(CFunctionArgument.Raw("double input")),
            // Random code to test that float.h has been #included
            CodeLines.from("return input * FLT_ROUNDS;"),
            Set(CFunction2.StandardHeader("float.h"))
          ),
          CFunction2(
            s"func3_${id}",
            Seq(CFunctionArgument.Raw("double input")),
            // Random code to test that time.h has been #included
            CodeLines.from("return cos(input + double(CLOCKS_PER_SEC));"),
            Set(CFunction2.StandardHeader("time.h"))
          )
        )
      }
    }
  }
}
