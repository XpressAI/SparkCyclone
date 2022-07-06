package io.sparkcyclone.native.transpiler

import io.sparkcyclone.native.transpiler.SyntaxTreeOps._
import scala.reflect.runtime.universe._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class FunctionReformatterUnitSpec extends AnyWordSpec {
  "FunctionReformatter" should {
    "correctly reformat Function expressions with a Tuple input" in {
      // Function that takes a 4-tuple
      val expr1 = reify { (x: (Int, Float, Long, Double)) => (x._1 + x._4) / x._2 }

      // Reformatting should produce a function that takes 4 arguments
      val func1 = FunctionReformatter.reformatFunction(expr1)
      func1.argTypes should be (List(typeOf[Int], typeOf[Float], typeOf[Long], typeOf[Double]))

      // Manually create the equivalent generated function
      val expr2 = reify { (in_1_val: Int, in_2_val: Float, in_3_val: Long, in_4_val: Double) => (in_1_val + in_4_val) / in_2_val }
      val func2 = CompilerToolBox.get.typecheck(expr2.tree).asInstanceOf[Function]

      // The new Function should be tree-equivalent to its tuple expansion
      func1.canEqual(func2) should be (true)
      CppTranspiler.evalMapFunc(func1) should be (CppTranspiler.evalMapFunc(func2))
    }
  }
}