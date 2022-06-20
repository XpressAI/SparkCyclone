package com.nec.native

import com.nec.native.transpiler.CppTranspilerSpec
import com.nec.spark.agile.core.VeNullableInt
import com.nec.spark.agile.join.SimpleEquiJoinFunction

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
