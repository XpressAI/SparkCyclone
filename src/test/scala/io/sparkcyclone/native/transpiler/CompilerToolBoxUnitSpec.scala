package io.sparkcyclone.native.transpiler

import io.sparkcyclone.native.transpiler.SyntaxTreeOps._
import scala.reflect.runtime.universe._
import java.time.Instant
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class CompilerToolBoxUnitSpec extends AnyWordSpec {
  "CompilerToolBox" should {
    "prime `scala.tools.reflect.ToolBox` so that it can typecheck functions correctly" in {
      // Load the toolbox
      val toolbox = CompilerToolBox.get

      // Run the first typecheck of a function with non-primitive type params
      toolbox.typecheck(reify { (x: Instant) => x.getNano }.tree).asInstanceOf[Function].returnType

      // Ensure that subsequent typechecks of functions works correctly
      toolbox.typecheck(reify { (x: Instant, y: Int) => y + 4 }.tree).asInstanceOf[Function].returnType =:= typeOf[Int] should be (true)
    }
  }
}
