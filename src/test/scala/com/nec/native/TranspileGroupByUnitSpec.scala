package com.nec.native

import scala.reflect.runtime.universe._

final class TranspileGroupByUnitSpec extends CppTranspilerSpec {
  "CppTranspiler for GroupBy Functions" should {
    "correctly transpile GroupBy identity" in {
      val groupByCode = CppTranspiler.transpileGroupBy(reify({ x: Long => x }))
      println(groupByCode.func.toCodeLinesWithHeaders.cCode)
    }

    "correctly transpile GroupBy identity of tuple element" in {
    val groupByCode = CppTranspiler.transpileGroupBy(reify({ x: (Long, Long) => x._1 }))
    println(groupByCode.func.toCodeLinesWithHeaders.cCode)
  }

    "correctly transpile GroupBy Long -> Long" in {
      val groupByCode = CppTranspiler.transpileGroupBy(reify({ x: Long => x * 2 }))
      println(groupByCode.func.toCodeLinesWithHeaders.cCode)
    }

    "correctly transpile GroupBy (Long, Long) -> Long" in {
      val groupByCode = CppTranspiler.transpileGroupBy(reify({ x: (Long, Long) => x._1 % 2 + x._2 % 3 }))
      println(groupByCode.func.toCodeLinesWithHeaders.cCode)
    }
  }
}
