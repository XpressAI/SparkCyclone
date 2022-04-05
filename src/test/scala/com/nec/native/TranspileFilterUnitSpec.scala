package com.nec.native

import scala.reflect.runtime.universe._
import java.time.Instant

final class TranspileFilterUnitSpec extends CppTranspilerSpec {
  "CppTranspiler for Filter Functions" should {
    "ensure proper order of arithmetic operations" in {
      val gencode = CppTranspiler.transpileFilter(reify( (a: Int) => a % 3 == 0 && a % 5 == 0 && a % 15 == 0))
      assert(supertrim(gencode.func.body.cCode).contains(supertrim("((((in_1_val % 3) == 0) && ((in_1_val % 5) == 0)) && ((in_1_val % 15) == 0))")))
    }

    "correctly filter by comparisons" in {
      val genCodeLT = CppTranspiler.transpileFilter(reify( (x: Int) => x < x*x - x))
      val genCodeGT = CppTranspiler.transpileFilter(reify( (x: Int) => x > 10))
      val genCodeLTE = CppTranspiler.transpileFilter(reify( (x: Int) => x <= x*x - x))
      val genCodeGTE = CppTranspiler.transpileFilter(reify( (x: Int) => x >= 10))
      val genCodeEq = CppTranspiler.transpileFilter(reify( (x: Int) => x == x*x-2))
      val genCodeNEq = CppTranspiler.transpileFilter(reify( (x: Int) => x != x*x-2))
      assertCodeEqualish(genCodeLT, CppSources.testFilterLTConstant)
      assertCodeEqualish(genCodeLTE, CppSources.testFilterLTEConstant)
      assertCodeEqualish(genCodeGT, CppSources.testFilterGTConstant)
      assertCodeEqualish(genCodeGTE, CppSources.testFilterGTEConstant)
      assertCodeEqualish(genCodeEq, CppSources.testFilterEq)
      assertCodeEqualish(genCodeNEq, CppSources.testFilterNEq)
    }

    "correctly filter with modulo operations" in {
      val genCodeMod = CppTranspiler.transpileFilter(reify( (x: Int) => x % 2 == 0))
      assertCodeEqualish(genCodeMod, CppSources.testFilterMod)
    }

    "correctly filter by inverse predicate" in {
      val genCodeInverse = CppTranspiler.transpileFilter(reify( (x: Int) => !(x % 2 == 0)))
      assertCodeEqualish(genCodeInverse, CppSources.testFilterInverse)
    }

    "correctly filter with combinations of || or &&" in {
      val genCodeCombinedAnd = CppTranspiler.transpileFilter(reify( (x: Long) => (x > 10) && (x < 15)))
      val genCodeCombinedOr =  CppTranspiler.transpileFilter(reify( (x: Long) => (x < 10) || (x > 15)))
      assertCodeEqualish(genCodeCombinedAnd, CppSources.testFilterAnd)
      assertCodeEqualish(genCodeCombinedOr, CppSources.testFilterOr)
    }

    "correctly filter java.time.Instants" in {
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

    "correctly filter with tuple arguments, e.g. (Long, Long) -> Boolean" in {
      val expected =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | int64_t in_1_val {};
          | int64_t in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   bitmask[i] = (((in_1_val % 2) + (in_2_val % 3)) == 0);
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
          | out_1[0] = in_2[0]->select(matching_ids);
        """.stripMargin

      val genCode = CppTranspiler.transpileFilter(reify({ x: (Long, Long) => (x._1 % 2 + x._2 % 3) == 0 }))
      assertCodeEqualish(genCode, expected)
    }

    "foo" in {
      val expected =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | int64_t in_1_val {};
          | double in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   bitmask[i] = ((float( (in_1_val + 2) ) * 3.14) < 6.34);
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
          | out_1[0] = in_2[0]->select(matching_ids);
        """.stripMargin

      val genCode = CppTranspiler.transpileFilter(reify { x: (Long, Double) => (x._1 + 2).toFloat * 3.14 < 6.34 })
      assertCodeEqualish(genCode, expected)
    }
  }
}

object CppSources {
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
