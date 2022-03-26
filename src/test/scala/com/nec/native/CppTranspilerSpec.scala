package com.nec.native

import org.scalatest.freespec.AnyFreeSpec

final class CppTranspilerSpec extends AnyFreeSpec {

  import scala.reflect.runtime.universe.reify

  val intKlass: Class[Int] = classOf[Int]

  val longKlass: Class[Long] = classOf[Long]

  // ignore *all* whitespaces (do not try to compile after this)
  def supertrim(s: String) = s.filter(!_.isWhitespace)

  def assertCodeEqualish(code1: String, code2: String) = {
    assert(supertrim(code1) == supertrim(code2))
  }

  "simple function Int -> Int" in {
    val gencode = CppTranspiler.transpile(reify( (x: Int, y: Int) => x * y + 2 ), intKlass)
    assertCodeEqualish(gencode, cppSources.test01)
  }

  "trivial bool functions" in {
    val gencodeTrue = CppTranspiler.transpileFilter(reify( (x: Int) => true  ), intKlass)
    val gencodeFalse = CppTranspiler.transpileFilter(reify( (x: Int) => false ), intKlass)

    assertCodeEqualish(gencodeTrue, cppSources.testFilterTrivialBoolTrue)
    assertCodeEqualish(gencodeFalse, cppSources.testFilterTrivialBoolFalse)
  }

  "filter by comparing" in {
    val genCodeLT = CppTranspiler.transpileFilter(reify( (x: Int) => x < x*x - x), intKlass)
    val genCodeGT = CppTranspiler.transpileFilter(reify( (x: Int) => x > 10), intKlass)
    val genCodeLTE = CppTranspiler.transpileFilter(reify( (x: Int) => x <= x*x - x), intKlass)
    val genCodeGTE = CppTranspiler.transpileFilter(reify( (x: Int) => x >= 10), intKlass)
    val genCodeEq = CppTranspiler.transpileFilter(reify( (x: Int) => x == x*x-2), intKlass)
    val genCodeNEq = CppTranspiler.transpileFilter(reify( (x: Int) => x != x*x-2), intKlass)
    assertCodeEqualish(genCodeLT, cppSources.testFilterLTConstant)
    assertCodeEqualish(genCodeLTE, cppSources.testFilterLTEConstant)
    assertCodeEqualish(genCodeGT, cppSources.testFilterGTConstant)
    assertCodeEqualish(genCodeGTE, cppSources.testFilterGTEConstant)
    assertCodeEqualish(genCodeEq, cppSources.testFilterEq)
    assertCodeEqualish(genCodeNEq, cppSources.testFilterNEq)
  }

  "mod filter" in {
    val genCodeMod = CppTranspiler.transpileFilter(reify( (x: Int) => x % 2 == 0), intKlass)
    assertCodeEqualish(genCodeMod, cppSources.testFilterMod)
  }

  "filter inverse of something" in {
    val genCodeInverse = CppTranspiler.transpileFilter(reify( (x: Int) => !(x % 2 == 0)), intKlass)
    assertCodeEqualish(genCodeInverse, cppSources.testFilterInverse)
  }

  "filter combined with || or &&" in {
    val genCodeCombinedAnd = CppTranspiler.transpileFilter(reify( (x: Long) => (x > 10) && (x < 15)), longKlass)
    val genCodeCombinedOr =  CppTranspiler.transpileFilter(reify( (x: Long) => (x < 10) || (x > 15)), longKlass)
    assertCodeEqualish(genCodeCombinedAnd, cppSources.testFilterAnd)
    assertCodeEqualish(genCodeCombinedOr, cppSources.testFilterOr)
  }

}

object cppSources {

  val test01 =
    """
      |size_t len = x_in[0]->count;
      |out[0] = nullable_int_vector::allocate();
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
      |  out[0] = nullable_int_vector::allocate();
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
      |  out[0] = nullable_int_vector::allocate();
      |  out[0]->resize(0);
      |  out[0]->set_validity(0, 0);
      |""".stripMargin

 val testFilterLTConstant =
   """
     |    size_t len = x_in[0]->count;
     |  size_t actual_len = 0;
     |  out[0] = nullable_int_vector::allocate();
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

  val testFilterLTEConstant =
    """
      |    size_t len = x_in[0]->count;
      |  size_t actual_len = 0;
      |  out[0] = nullable_int_vector::allocate();
      |  out[0]->resize(len);
      |  int32_t x{};
      |  for (int i = 0; i < len; i++) {
      |    x = x_in[0]->data[i];
      |    if ( (x <= ((x * x) - x)) ) {
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
      |  out[0] = nullable_int_vector::allocate();
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

  val testFilterGTEConstant =
    """   size_t len = x_in[0]->count;
      |  size_t actual_len = 0;
      |  out[0] = nullable_int_vector::allocate();
      |  out[0]->resize(len);
      |  int32_t x{};
      |  for (int i = 0; i < len; i++) {
      |    x = x_in[0]->data[i];
      |    if ( (x >= 10) ) {
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
      |  out[0] = nullable_int_vector::allocate();
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

  val testFilterNEq =
    """   size_t len = x_in[0]->count;
      |  size_t actual_len = 0;
      |  out[0] = nullable_int_vector::allocate();
      |  out[0]->resize(len);
      |  int32_t x{};
      |  for (int i = 0; i < len; i++) {
      |    x = x_in[0]->data[i];
      |    if ( (x != ((x * x) - 2)) ) {
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
      |  out[0] = nullable_int_vector::allocate();
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
      |  out[0] = nullable_int_vector::allocate();
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

  val testFilterAnd =
    """
      |size_t len = x_in[0]->count;
      |  size_t actual_len = 0;
      |  out[0] = nullable_bigint_vector::allocate();
      |  out[0]->resize(len);
      |  int64_t x{};
      |  for (int i = 0; i < len; i++) {
      |    x = x_in[0]->data[i];
      |    if ( ((x > 10) && (x < 15)) ) {
      |      out[0]->data[actual_len++] = x;
      |    }
      |  }
      |  for (int i=0; i < actual_len; i++) {
      |    out[0]->set_validity(i, 1);
      |  }
      |  out[0]->resize(actual_len);
      |""".stripMargin

  val testFilterOr =
    """
      |size_t len = x_in[0]->count;
      |  size_t actual_len = 0;
      |  out[0] = nullable_bigint_vector::allocate();
      |  out[0]->resize(len);
      |  int64_t x{};
      |  for (int i = 0; i < len; i++) {
      |    x = x_in[0]->data[i];
      |    if ( ((x < 10) || (x > 15)) ) {
      |      out[0]->data[actual_len++] = x;
      |    }
      |  }
      |  for (int i=0; i < actual_len; i++) {
      |    out[0]->set_validity(i, 1);
      |  }
      |  out[0]->resize(actual_len);
      |""".stripMargin
}
