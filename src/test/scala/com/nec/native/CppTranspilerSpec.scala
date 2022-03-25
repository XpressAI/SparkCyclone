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

  "filter by comparing" in {
    val genCodeLT = CppTranspiler.transpileFilter(reify( (x: Int) => x < x*x - x))
    val genCodeGT = CppTranspiler.transpileFilter(reify( (x: Int) => x > 10))
    val genCodeEq = CppTranspiler.transpileFilter(reify( (x: Int) => x == x*x-2))
    val genCodeNEq = CppTranspiler.transpileFilter(reify( (x: Int) => x != x*x-2))
    assertCodeEqualish(genCodeLT, cppSources.testFilterLTConstant)
    assertCodeEqualish(genCodeGT, cppSources.testFilterGTConstant)
    assertCodeEqualish(genCodeEq, cppSources.testFilterEq)
  }

  "mod filter" in {
    val genCodeMod = CppTranspiler.transpileFilter(reify( (x: Int) => x % 2 == 0))
    assertCodeEqualish(genCodeMod, cppSources.testFilterMod)
  }

  "filter inverse of something" in {
    val genCodeInverse = CppTranspiler.transpileFilter(reify( (x: Int) => !(x % 2 == 0)))
    assertCodeEqualish(genCodeInverse, cppSources.testFilterInverse)
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

     val testFilterLTConstant =
       """
         |    size_t len = x_in[0]->count;
         |  size_t actual_len = 0;
         |  out[0] = nullable_bigint_vector::allocate();
         |  out[0]->resize(len);
         |  int32_t x{};
         |  for (int i = 0; i < len; i++) {
         |    x = x_in[0]->data[i];
         |    if ( (x < ((x * x) - x)) ) {
         |      out[0]->data[actual_len++] = x;
         |    }
         |  }
         |  for (int i=0; i < actual_len; i++) {
         |    out[0]->set_validity(i, 1);
         |  }
         |  out[0]->resize(actual_len);
         |""".stripMargin

      val testFilterGTConstant =
        """   size_t len = x_in[0]->count;
          |  size_t actual_len = 0;
          |  out[0] = nullable_bigint_vector::allocate();
          |  out[0]->resize(len);
          |  int32_t x{};
          |  for (int i = 0; i < len; i++) {
          |    x = x_in[0]->data[i];
          |    if ( (x > 10) ) {
          |      out[0]->data[actual_len++] = x;
          |    }
          |  }
          |  for (int i=0; i < actual_len; i++) {
          |    out[0]->set_validity(i, 1);
          |  }
          |  out[0]->resize(actual_len);
          |  """.stripMargin

      val testFilterEq =
        """   size_t len = x_in[0]->count;
          |  size_t actual_len = 0;
          |  out[0] = nullable_bigint_vector::allocate();
          |  out[0]->resize(len);
          |  int32_t x{};
          |  for (int i = 0; i < len; i++) {
          |    x = x_in[0]->data[i];
          |    if ( (x == ((x * x) - 2)) ) {
          |      out[0]->data[actual_len++] = x;
          |    }
          |  }
          |  for (int i=0; i < actual_len; i++) {
          |    out[0]->set_validity(i, 1);
          |  }
          |  out[0]->resize(actual_len);
          |  """.stripMargin


      val testFilterMod =
        """
          |size_t len = x_in[0]->count;
          |  size_t actual_len = 0;
          |  out[0] = nullable_bigint_vector::allocate();
          |  out[0]->resize(len);
          |  int32_t x{};
          |  for (int i = 0; i < len; i++) {
          |    x = x_in[0]->data[i];
          |    if ( ((x % 2) == 0) ) {
          |      out[0]->data[actual_len++] = x;
          |    }
          |  }
          |  for (int i=0; i < actual_len; i++) {
          |    out[0]->set_validity(i, 1);
          |  }
          |  out[0]->resize(actual_len);
          |""".stripMargin


  val testFilterInverse =
    """
      |  size_t len = x_in[0]->count;
      |  size_t actual_len = 0;
      |  out[0] = nullable_bigint_vector::allocate();
      |  out[0]->resize(len);
      |  int32_t x{};
      |  for (int i = 0; i < len; i++) {
      |    x = x_in[0]->data[i];
      |    if (  !((x % 2) == 0) ) {
      |      out[0]->data[actual_len++] = x;
      |    }
      |  }
      |  for (int i=0; i < actual_len; i++) {
      |    out[0]->set_validity(i, 1);
      |  }
      |  out[0]->resize(actual_len);
      |  """.stripMargin
}
