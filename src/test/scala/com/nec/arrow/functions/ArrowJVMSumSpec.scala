package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMSumSpec extends AnyFreeSpec {
  "JVM sum works" in {
    val inputData: Seq[Seq[Double]] =
      Seq(Seq(1.0, 2.0, 3.0), Seq(10.0, 20.0, 30.0), Seq(50.0, 60.0, 70.0))

    withArrowFloat8Vector(inputData) { vcv =>
      assert(Sum.sumJVM(vcv, 3) == Seq(61.0, 82.0, 103.0))
    }
  }
}
