package com.nec.spark.agile

import com.nec.spark.AcceptanceTest
import com.nec.spark.agile.BigDecimalSummer.{
  readBigDecimal,
  BundleNecSSHSummer,
  PythonNecSSHSummer,
  ScalaSummer
}
import com.nec.spark.agile.BigDecimalSummerSpec.stubSummer
import org.scalatest.freespec.AnyFreeSpec

import scala.sys.process._

object BigDecimalSummerSpec {

  def stubSummer(fakeResult: BigDecimal): BigDecimalSummer =
    _ => readBigDecimal(Seq("ssh", "a6", "echo", fakeResult.toString()).!!)

}

final class BigDecimalSummerSpec extends AnyFreeSpec {

  private def sumTest(summer: BigDecimalSummer): Unit =
    assert(summer.sum(List(1, 2, 3, 4)) == BigDecimal(10))

  "It works in-JVM" in {
    sumTest(ScalaSummer)
  }

  "It works (echo stub)" in {
    sumTest(stubSummer(fakeResult = 10))
  }

  "It works (Python)" taggedAs AcceptanceTest in {
    sumTest(PythonNecSSHSummer)
  }

  "It works with a larger number" taggedAs AcceptanceTest in {
    assert(PythonNecSSHSummer.sum(List(1, 2, 3, 4, 1031858758.000)) == 1031858768)
  }

  "It works (VE)" taggedAs AcceptanceTest in {
    sumTest(BundleNecSSHSummer)
  }

}
