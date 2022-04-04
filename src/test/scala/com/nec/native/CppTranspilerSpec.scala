package com.nec.native

import org.scalatest.Assertion
import org.scalatest.freespec.AnyFreeSpec

import java.time.Instant

//noinspection ScalaUnusedSymbol
final class CppTranspilerSpec extends AnyFreeSpec {
  import scala.reflect.runtime.universe._

  // ignore *all* whitespaces (do not try to compile after this)
  def supertrim(s: String): String = s.filter(!_.isWhitespace)

  def assertCodeEqualish(code1: CompiledVeFunction, code2: String): Assertion = {
    assert(supertrim(code1.func.body.cCode) == supertrim(code2))
  }

  "simple function Int -> Int" in {
    val gencode = CppTranspiler.transpileMap(reify( (x: Int) => x * 2 ))
    assert(supertrim(gencode.func.body.cCode).contains("in_1_val*2"))
  }

  "Ensure proper operation order" in {
    val gencode = CppTranspiler.transpileMap(reify( (x: Int) => ((2 * x) + 12) - (x % 15)))
    assert(supertrim(gencode.func.body.cCode).contains("(((2*in_1_val)+12)-(in_1_val%15))"))
  }

  "Ensure filter has correct operation order" in {
    val gencode = CppTranspiler.transpileFilter(reify( (a: Int) => a % 3 == 0 && a % 5 == 0 && a % 15 == 0))
    assert(supertrim(gencode.func.body.cCode).contains(supertrim("((((in_1_val % 3) == 0) && ((in_1_val % 5) == 0)) && ((in_1_val % 15) == 0))")))
  }

  "filter by comparing" in {
    val genCodeLT = CppTranspiler.transpileFilter(reify( (x: Int) => x < x*x - x))
    val genCodeGT = CppTranspiler.transpileFilter(reify( (x: Int) => x > 10))
    val genCodeLTE = CppTranspiler.transpileFilter(reify( (x: Int) => x <= x*x - x))
    val genCodeGTE = CppTranspiler.transpileFilter(reify( (x: Int) => x >= 10))
    val genCodeEq = CppTranspiler.transpileFilter(reify( (x: Int) => x == x*x-2))
    val genCodeNEq = CppTranspiler.transpileFilter(reify( (x: Int) => x != x*x-2))
    assertCodeEqualish(genCodeLT, cppSources.testFilterLTConstant)
    assertCodeEqualish(genCodeLTE, cppSources.testFilterLTEConstant)
    assertCodeEqualish(genCodeGT, cppSources.testFilterGTConstant)
    assertCodeEqualish(genCodeGTE, cppSources.testFilterGTEConstant)
    assertCodeEqualish(genCodeEq, cppSources.testFilterEq)
    assertCodeEqualish(genCodeNEq, cppSources.testFilterNEq)
  }

  "mod filter" in {
    val genCodeMod = CppTranspiler.transpileFilter(reify( (x: Int) => x % 2 == 0))
    assertCodeEqualish(genCodeMod, cppSources.testFilterMod)
  }

  "filter inverse of something" in {
    val genCodeInverse = CppTranspiler.transpileFilter(reify( (x: Int) => !(x % 2 == 0)))
    assertCodeEqualish(genCodeInverse, cppSources.testFilterInverse)
  }

  "filter combined with || or &&" in {
    val genCodeCombinedAnd = CppTranspiler.transpileFilter(reify( (x: Long) => (x > 10) && (x < 15)))
    val genCodeCombinedOr =  CppTranspiler.transpileFilter(reify( (x: Long) => (x < 10) || (x > 15)))
    assertCodeEqualish(genCodeCombinedAnd, cppSources.testFilterAnd)
    assertCodeEqualish(genCodeCombinedOr, cppSources.testFilterOr)
  }

  "filter java.time.Instants" in {
    val output1 =
      """
        | size_t len = in_1[0]->count;
        | std::vector<size_t> bitmask(len);
        | int64_t in_1_val{};
        | for (auto i = 0; i < len; i++) {
        |   in_1_val = in_1[0]->data[i];
        |   bitmask[i] = (((in_1_val == 1648428244277340000) ? 0 : (in_1_val < 1648428244277340000) ? -1 : 1) < 0);
        | }
        | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
        | out_0[0] = in_1[0]->select(matching_ids);
        """.stripMargin

    val output2 =
      """
        | size_t len = in_1[0]->count;
        | std::vector<size_t> bitmask(len);
        | int64_t in_1_val{};
        | for (auto i = 0; i < len; i++) {
        |   in_1_val = in_1[0]->data[i];
        |   bitmask[i] = (((1648428244277340000 == in_1_val) ? 0 : (1648428244277340000 < in_1_val) ? -1 : 1) != 0);
        | }
        | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
        | out_0[0] = in_1[0]->select(matching_ids);
        """.stripMargin

    val genCode1 = CppTranspiler.transpileFilter(reify { x: Instant => x.compareTo(Instant.parse("2022-03-28T00:44:04.277340Z")) < 0 })
    val genCode2 = CppTranspiler.transpileFilter(reify { x: Instant => Instant.parse("2022-03-28T00:44:04.277340Z").compareTo(x) != 0 })

    assertCodeEqualish(genCode1, output1)
    assertCodeEqualish(genCode2, output2)
  }

  "map Long -> (Long, Long)" in {
    val genCode0 = CppTranspiler.transpileMap(reify { x: Long => x })
    val genCode1 = CppTranspiler.transpileMap(reify { x: Long => (x, x * 2) })
    val genCode2 = CppTranspiler.transpileMap(reify { x: (Long, Long) => x._2 })
    println(genCode0.func.toCodeLinesWithHeaders.cCode)
    println(genCode1.func.toCodeLinesWithHeaders.cCode)
    println(genCode2.func.toCodeLinesWithHeaders.cCode)
    //assert(!supertrim(genCode2.func.body.cCode).contains(""))
  }

  "groupBy Long -> Long" in {
    val groupByCode = CppTranspiler.transpileGroupBy(reify({ x: Long => x * 2 }))
    println(groupByCode.func.toCodeLinesWithHeaders.cCode)
  }

  "groupBy (Long, Long) -> Long" in {
    val groupByCode = CppTranspiler.transpileGroupBy(reify({ x: (Long, Long) => x._1 % 2 + x._2 % 3 }))
    println(groupByCode.func.toCodeLinesWithHeaders.cCode)
  }

  "filter (Long, Long) -> Boolean" in {
    val groupByCode = CppTranspiler.transpileFilter(reify({ x: (Long, Long) => (x._1 % 2 + x._2 % 3) == 0 }))
    println(groupByCode.func.toCodeLinesWithHeaders.cCode)
  }

  "reduce Long, Long -> Long" in {
    val groupByCode = CppTranspiler.transpileReduce(reify({ (x: Long, y: Long) => x + y }))
    println(groupByCode.func.toCodeLinesWithHeaders.cCode)
  }

  "reduce (Long, Long), (Long, Long) -> Long" in {
    val groupByCode = CppTranspiler.transpileReduce(reify({ (x: (Long, Long), y: (Long, Long)) => (x._1 + y._2, x._2 + y._1) }))
    println(groupByCode.func.toCodeLinesWithHeaders.cCode)
  }
}

object cppSources {
  val testFilterTrivialBoolTrue: String =
    """
      |  size_t len = in_1[0]->count;
      |  out_0[0] = nullable_int_vector::allocate();
      |  out_0[0]->resize(len);
      |  int32_t in_1_val{};
      |  for (auto i = 0; i < len; i++) {
      |    out_0[0]->data[i] = in_1[0]->data[i];
      |  }
      |  out_0[0]->set_validity(0, len);
      |""".stripMargin


  val testFilterTrivialBoolFalse: String =
    """
      |  size_t len = in_1[0]->count;
      |  out_0[0] = nullable_int_vector::allocate();
      |  out_0[0]->resize(0);
      |  out_0[0]->set_validity(0, 0);
      |""".stripMargin

 val testFilterLTConstant: String =
  """
    | size_t len = in_1[0]->count;
    | std::vector<size_t> bitmask(len);
    | int32_t in_1_val{};
    | for (auto i = 0; i < len; i++) {
    |   in_1_val = in_1[0]->data[i];
    |   bitmask[i] = (in_1_val < ((in_1_val * in_1_val) - in_1_val));
    | }
    | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
    | out_0[0] = in_1[0]->select(matching_ids);
  """.stripMargin


  val testFilterLTEConstant: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = (in_1_val <= ((in_1_val * in_1_val) - in_1_val));
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterGTConstant: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = (in_1_val > 10);
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterGTEConstant: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = (in_1_val >= 10);
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterEq: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = (in_1_val == ((in_1_val * in_1_val) - 2));
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterNEq: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = (in_1_val != ((in_1_val * in_1_val) - 2));
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterMod: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = ((in_1_val % 2) == 0);
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterInverse: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int32_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = !((in_1_val % 2) == 0);
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterAnd: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int64_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = ((in_1_val > 10) && (in_1_val < 15));
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin

  val testFilterOr: String =
    """
      | size_t len = in_1[0]->count;
      | std::vector<size_t> bitmask(len);
      | int64_t in_1_val{};
      | for (auto i = 0; i < len; i++) {
      |   in_1_val = in_1[0]->data[i];
      |   bitmask[i] = ((in_1_val < 10) || (in_1_val > 15));
      | }
      | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
      | out_0[0] = in_1[0]->select(matching_ids);
      """.stripMargin
}
