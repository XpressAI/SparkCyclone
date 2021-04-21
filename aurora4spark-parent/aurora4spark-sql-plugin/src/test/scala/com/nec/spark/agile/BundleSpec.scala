package com.nec.spark.agile

import com.nec.spark.agile.BundleSpec.sumBundle
import org.scalatest.freespec.AnyFreeSpec

object BundleSpec {
  def sumBundle: Bundle = Bundle.sumBigDecimals(List(1, 2, 3))
}
final class BundleSpec extends AnyFreeSpec {
  "The summing bundle contains 'print'" in {
    assert(sumBundle.asPythonScript.contains("print"))
  }
}
