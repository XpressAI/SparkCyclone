package com.nec.cmake.functions

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CodeStructure
import com.nec.spark.agile.CodeStructure.CodeSection.{Header, NonHeader}
import org.scalatest.freespec.AnyFreeSpec

object CombineSourcesSpec {}

final class CombineSourcesSpec extends AnyFreeSpec {
  "Simple include is parsed" in {
    assert(
      CodeStructure.from(CodeLines.from("#include x")) == CodeStructure(
        List(Header(CodeLines.from("#include x")))
      )
    )
  }
  "Simple include is parsed with some headroom in front" in {
    assert(
      CodeStructure.from(CodeLines.from("", "#include x")) == CodeStructure(
        List(Header(CodeLines.from("#include x")))
      )
    )
  }
  "Simple ifdef section is parsed" in {
    assert(
      CodeStructure.from(CodeLines.from("#ifdef x", "y", "#endif")) == CodeStructure(
        List(Header(CodeLines.from("#ifdef x", "y", "#endif")))
      )
    )
  }
  "Simple code is parsed" in {
    assert(
      CodeStructure.from(CodeLines.from("x")) == CodeStructure(List(NonHeader(CodeLines.from("x"))))
    )
  }
  "Combination of include and code is parsed" in {
    assert(
      CodeStructure.from(CodeLines.from("#include x", "x")) == CodeStructure(
        List(Header(CodeLines.from("#include x")), NonHeader(CodeLines.from("x")))
      )
    )
  }

  "CodeLines can parse from set of lines" in {
    assert(CodeLines.parse("x\ny\r\nz") == CodeLines.from("x", "y", "z"))
  }

  "Final combination is parsed" in {
    val setOps =
      """#include "frovedis/core/set_operations.hpp""""

    val zz = """#include <zz>"""

    val f1 = """extern "C" long amplify_flt_eval_2099938059 ("""
    val f2 = """extern "C" long amplify_flt_eval_2099938054 ("""

    val sources = List[CodeLines](CodeLines.from(setOps, f1), CodeLines.from(zz, setOps, f2))

    val result = CodeStructure.combine(sources.map(cl => CodeStructure.from(cl)))
    assert(result == CodeLines.from(setOps, zz, f1, f2))
  }
}
