package com.nec.native

import com.nec.spark.agile.core.{CFunction2, CodeLines}
import com.nec.spark.agile.core.CFunction2.CFunctionArgument
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
          CodeLines.from(s"return input * func1_${id}(input);")
        )
      }

      lazy val secondary: Seq[CFunction2] = {
        Seq(CFunction2(
          s"func2_${id}",
          Seq(CFunctionArgument.Raw("double input")),
          CodeLines.from("return input * 2;"),
          Set(CFunction2.StandardHeader("float.h"))
        ))
      }
    }
  }
}
