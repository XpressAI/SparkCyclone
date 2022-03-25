package com.nec.native

import org.scalatest.freespec.AnyFreeSpec

final class CppTranspilerSpec extends AnyFreeSpec {

  import scala.reflect.runtime.universe.reify

  // ignore *all* whitespaces (do not try to compile after this)
  def supertrim(s: String) = s.filter(!_.isWhitespace)

  def assertCodeEqualish(code1: String, code2: String) = {
    assert(supertrim(code1) == supertrim(code2))
  }

  "simple function Int -> Int" in {
    val gencode = CppTranspiler.transpile(reify( (x: Int, y: Int) => x * y + 2 ))
    assertCodeEqualish(gencode, cppSources.test01)
  }

  "trivial bool functions" in {
    val gencodeTrue = CppTranspiler.transpileFilter(reify( (x: Int) => true  ))
    val gencodeFalse = CppTranspiler.transpileFilter(reify( (x: Int) => false ))

    assertCodeEqualish(gencodeTrue, cppSources.testFilterTrivialBoolTrue)
    assertCodeEqualish(gencodeFalse, cppSources.testFilterTrivialBoolFalse)
  }
}

object cppSources {

  val test01 =
    """
      |size_t len = x_in[0]->count;
      |out[0] = nullable_bigint_vector::allocate();
      |out[0]->resize(len);
      |int32_t x{};
      |int32_t y{};
      |for (int i = 0; i < len; i++) {
      |  x = x_in[0]->data[i];
      |  y = y_in[0]->data[i];
      |  out[0]->data[i] = ((x * y) + 2);
      |  out[0]->set_validity(i, 1);
      |}
      |""".stripMargin

    val testFilterTrivialBoolTrue =
      """
        |  size_t len = x_in[0]->count;
        |  out[0] = nullable_bigint_vector::allocate();
        |  out[0]->resize(len);
        |  int32_t x{};
        |  for (int i = 0; i < len; i++) {
        |    out[0]->data[i] = x_in[0]->data[i];
        |  }
        |  out[0]->set_validity(0, len);
        |""".stripMargin


    val testFilterTrivialBoolFalse =
      """
        |  size_t len = x_in[0]->count;
        |  out[0] = nullable_bigint_vector::allocate();
        |  out[0]->resize(0);
        |  out[0]->set_validity(0, 0);
        |""".stripMargin

}
