package com.nec.native.transpiler

import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

trait CppTranspilerSpec extends AnyWordSpec {
  def supertrim(s: String): String = {
    s.filter(!_.isWhitespace)
  }

  def assertCodeEqualish(vefunc: CompiledVeFunction, expected: String): Assertion = {
    assert(supertrim(vefunc.func.body.cCode) == supertrim(expected))
  }
}
