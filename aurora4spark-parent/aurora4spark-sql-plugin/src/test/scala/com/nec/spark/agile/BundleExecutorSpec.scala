package com.nec.spark.agile

import com.nec.spark.AcceptanceTest
import org.scalatest.freespec.AnyFreeSpec

final class BundleExecutorSpec extends AnyFreeSpec {
  "It runs a basic Python script" taggedAs AcceptanceTest in {
    assert(
      BundleExecutor
        .executePython("print('hey')\n")
        .mkString
        .trim == "hey"
    )
  }
  "We can run our bundle of sums and it returns the expected output" taggedAs AcceptanceTest in {
    assert(
      BundleExecutor.returningBigDecimal
        .executeBundle(Bundle.sumBigDecimals(List(1, 2, 3))) == BigDecimal(6)
    )
  }

  "We can run our bundle of sums and it returns the expected output (2)" taggedAs AcceptanceTest in {
    assert(
      BundleExecutor.returningBigDecimal
        .executeBundle(Bundle.sumBigDecimals(List(1, 2, 3, 4))) == BigDecimal(10)
    )
  }

  "We can run our bundle of sums in pure Python and it returns the expected output" taggedAs AcceptanceTest in {
    assert(
      BundleExecutor.returningBigDecimal
        .executeBundle(Bundle.sumBigDecimalsPurePython(List(1, 2, 3, 4))) == BigDecimal(10)
    )
  }
}
