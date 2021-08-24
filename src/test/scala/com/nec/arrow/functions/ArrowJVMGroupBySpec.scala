package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.{withDirectFloat8Vector, withDirectIntVector}
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMGroupBySpec extends AnyFreeSpec {
  "JVM sum works" in {
    val groupingColumn: Seq[Double] =
      Seq(10.0, 20.0, 30.0, 40.0, 10.0, 20.0, 50.0)
    val valuesColumn: Seq[Double] =
      Seq(100.0, 200.0, 300.0, 400.0, 500.0, 900.0, 1000.0)

    val expectedResult = Map(
      10.0 -> Seq(100.0, 500.0),
      20.0 -> Seq(200.0, 900.0),
      30.0 -> Seq(300.0),
      40.0 -> Seq(400.0),
      50.0 -> Seq(1000.0)
    )
    withDirectFloat8Vector(groupingColumn) { groupingColumnVec =>
      withDirectFloat8Vector(valuesColumn) { valuesColumnVec =>
        assert(GroupBy.groupJVM(groupingColumnVec, valuesColumnVec) == expectedResult)
      }
    }
  }
}
