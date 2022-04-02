package com.nec.native

import com.nec.native.SyntaxTreeOps._
import scala.reflect.runtime.universe._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class FunctionReformatterUnitSpec extends AnyWordSpec {
  "FunctionReformatter" should {
    "correctly reformat Function expressions with a Tuple input" in {
      // Function that takes a 4-tuple
      val expr1 = reify { (x: (Int, Float, Long, Double)) => (x._1 + x._4) / x._2 }

      // Reformatting should produce a function that takes 4 arguments
      val func = FunctionReformatter.reformatFunction(expr1)
      func.argTypes should be (List(typeOf[Int], typeOf[Float], typeOf[Long], typeOf[Double]))

      // The new Function should be tree-equivalent to its tuple expansion
      val expr2 = reify { (a: Int, b: Float, c: Long, d: Double) => (a + d) / b }
      func.canEqual(expr2.tree) should be (true)
    }
  }
}
