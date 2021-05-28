package com.nec.spark.agile

import com.nec.spark.agile.BigDecimalSummer.ScalaSummer
import org.scalatest.freespec.AnyFreeSpec

final class BigDecimalSummerSpec extends AnyFreeSpec {

  private def sumTest(summer: BigDecimalSummer): Unit =
    assert(summer.sum(List(1, 2, 3, 4)) == BigDecimal(10))

  "It works in-JVM" in {
    sumTest(ScalaSummer)
  }

}
