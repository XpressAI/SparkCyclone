package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMGroupBySumSpec extends AnyFreeSpec {
  "JVM sum works" in {
    val groupingColumn: Seq[Double] =
      Seq(10.0, 20.0, 30.0, 40.0, 10.0, 20.0, 50.0)
    val valuesColumn: Seq[Double] =
      Seq(100.0, 200.0, 300.0, 400.0, 500.0, 900.0, 1000.0)

    val expectedResult = Map(
      10.0 -> 600.0,
      20.0 -> 1100.0,
      30.0 -> 300.0,
      40.0 -> 400.0,
      50.0 -> 1000.0,

    )
    withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
      withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
        assert(
          GroupBySum.groupJVM(groupingColumnVec, valuesColumnVec) == expectedResult
        )
      }
    }
  }
}
