package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.{withArrowFloat8Vector, withDirectFloat8Vector}
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMSortSpec extends AnyFreeSpec {
  "JVM sum works" in {
    val inputData: Seq[Double] =
      Seq(100.0, 10.0, 20.0, 8.0, 900.0, 1000.0, 1.0)

    withDirectFloat8Vector(inputData) { vcv =>
      assert(Sort.sortJVM(vcv) == Seq(1.0, 8.0, 10.0, 20.0, 100.0, 900.0, 1000.0))
    }
  }
}
