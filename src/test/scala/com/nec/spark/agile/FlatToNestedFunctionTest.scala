package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration.CExpression
import org.scalatest.freespec.AnyFreeSpec

final class FlatToNestedFunctionTest extends AnyFreeSpec {
  "It works for 3" in {
    assert(
      FlatToNestedFunction.nest(Seq("a", "b", "c"), "std::max") == s"std::max(a, std::max(b, c))"
    )
  }

  "It works for 2" in {
    assert(FlatToNestedFunction.nest(Seq("a", "b"), "std::max") == s"std::max(a, b)")
  }

  "In case of 2 items, it checks their nulls" in {
    assert(
      FlatToNestedFunction.runWhenNotNull(
        items = List(
          CExpression(cCode = "a", isNotNullCode = Some("1")),
          CExpression(cCode = "b", isNotNullCode = Some("2"))
        ),
        function = "std::max"
      ) == CExpression(
        cCode = "(1 && 2) ? (std::max(a, b)) : (1 ? a : b)",
        isNotNullCode = Some("1 || 2")
      )
    )
  }
  "In case of 2 items, only 1 nullable" in {
    assert(
      FlatToNestedFunction.runWhenNotNull(
        items = List(
          CExpression(cCode = "a", isNotNullCode = None),
          CExpression(cCode = "b", isNotNullCode = Some("2"))
        ),
        function = "std::max"
      ) == CExpression(cCode = "(2) ? (std::max(a, b)) : a", isNotNullCode = None)
    )
  }
}
