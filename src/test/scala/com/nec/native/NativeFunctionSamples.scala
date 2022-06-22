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

      lazy val func: CFunction2 = {
        CFunction2(
          s"func_${Random.nextInt(1000)}",
          Seq(CFunctionArgument.Raw("double input")),
          CodeLines.from("return input * 2;")
        )
      }
    }
  }
}
