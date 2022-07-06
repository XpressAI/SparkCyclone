package io.sparkcyclone.native

import io.sparkcyclone.native.transpiler.CppTranspilerSpec
import io.sparkcyclone.spark.agile.core.VeNullableInt
import io.sparkcyclone.spark.agile.join.SimpleEquiJoinFunction

final class SimpleEquiJoinFunctionUnitSpec extends CppTranspilerSpec {
  "SimpleEuqiJoinFunction" should {
    "correctly produce join code" in {
      val func = SimpleEquiJoinFunction(
        "function_name",
        List(VeNullableInt, VeNullableInt),
        List(VeNullableInt, VeNullableInt, VeNullableInt)
      )
      println(func.toCFunction.toCodeLinesWithHeaders.cCode)
    }
  }
}
