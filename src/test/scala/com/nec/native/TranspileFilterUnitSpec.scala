package com.nec.native

import scala.reflect.runtime.universe._
import java.time.Instant

final class TranspileFilterUnitSpec extends CppTranspilerSpec {
  "CppTranspiler for Filter Functions" should {
    "correctly transpile a set of arithmetic operations while preserving the order of operations" in {
      val gencode = CppTranspiler.transpileFilter(reify( (a: Int) => a % 3 == 0 && a % 5 == 0 && a % 15 == 0))
      assert(supertrim(gencode.func.body.cCode).contains(supertrim("((((in_1_val % 3) == 0) && ((in_1_val % 5) == 0)) && ((in_1_val % 15) == 0))")))
    }

    "correctly transpile basic comparisons" in {
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

    "correctly transpile modulo operations" in {
      val genCodeMod = CppTranspiler.transpileFilter(reify( (x: Int) => x % 2 == 0))
      assertCodeEqualish(genCodeMod, CppSources.testFilterMod)
    }

    "correctly transpile predicate inverses" in {
      val genCodeInverse = CppTranspiler.transpileFilter(reify( (x: Int) => !(x % 2 == 0)))
      assertCodeEqualish(genCodeInverse, CppSources.testFilterInverse)
    }

    "correctly transpile combinations of || or &&" in {
      val genCodeCombinedAnd = CppTranspiler.transpileFilter(reify( (x: Long) => (x > 10) && (x < 15)))
      val genCodeCombinedOr =  CppTranspiler.transpileFilter(reify( (x: Long) => (x < 10) || (x > 15)))
      assertCodeEqualish(genCodeCombinedAnd, CppSources.testFilterAnd)
      assertCodeEqualish(genCodeCombinedOr, CppSources.testFilterOr)
    }

    "correctly transpile a set of basic arithmetic operations on Tuple arguments" in {
      val output1 =
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

      val genCode1 = CppTranspiler.transpileFilter(reify({ x: (Long, Long) => (x._1 % 2 + x._2 % 3) == 0 }))
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile an Ident Body for both the non-Tuple and Tuple input cases" in {
      val output1 = "out_0[0] = in_1[0]->clone();"
      val output2 = "out_0[0] = in_2[0]->clone();"

      val genCode1 = CppTranspiler.transpileFilter(reify { (x: Boolean) => x })
      val genCode2 = CppTranspiler.transpileFilter(reify { (x: (Float, Boolean)) => x._2 })
      assertCodeEqualish(genCode1, output1)
      assertCodeEqualish(genCode2, output2)
    }

    "correctly transpile an Apply Body with primitive type conversions" in {
      val output1 =
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

      val genCode1 = CppTranspiler.transpileFilter(reify { x: (Long, Double) => (x._1 + 2).toFloat * 3.14 < 6.34 })
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile a Select Body" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | float in_1_val {};
          | int32_t in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   bitmask[i] =  !in_2_val;
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
          | out_1[0] = in_2[0]->select(matching_ids);
        """.stripMargin

      val genCode1 = CppTranspiler.transpileFilter(reify { (x: (Float, Boolean)) => !x._2 })
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile an If Body" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | int64_t in_1_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |
          |   bitmask[i] = (
          |     ({
          |       const int32_t x = 3;
          |       ((in_1_val % x) == 0);
          |     })
          |   ) ? (
          |     (in_1_val < 43)
          |   ) : (
          |     (in_1_val == 92612)
          |   );
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
        """.stripMargin

      val genCode1 = CppTranspiler.transpileFilter(reify { (a: Long) => if ({ val x = 3; a % x == 0}) a < 43 else a == 92612 })
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile a Literal Constant Body" in {
      val output1 =
        """
          | out_0[0] = in_1->clone();
          | out_1[0] = in_2->clone();
          | out_2[0] = in_3->clone();
        """.stripMargin

      val output2 =
        """
          | out_0[0] = nullable_double_vector::allocate();
          | out_0[0]->resize(0);
          | out_1[0] = nullable_bigint_vector::allocate();
          | out_1[0]->resize(0);
        """.stripMargin

      val genCode1 = CppTranspiler.transpileFilter(reify { (a: (Double, Long, Float)) => true })
      val genCode2 = CppTranspiler.transpileFilter(reify { (a: (Double, Long)) => false })
      assertCodeEqualish(genCode1, output1)
      assertCodeEqualish(genCode2, output2)
    }

    "correctly transpile a Block Body with ValDefs, Assigns, and Ifs" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | float in_1_val {};
          | int64_t in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   int32_t x = (int32_t( in_1_val ) % 3);
          |
          |   const int32_t y =  ({
          |     const int32_t z = 66;
          |     (
          |       ({
          |         x = (x + 1);
          |         (x < z);
          |       })
          |     ) ? (
          |       10
          |     ) : (
          |       ((x > z)) ? (-10) : (2)
          |     );
          |   });
          |
          |   x = (x + y);
          |
          |   bitmask[i] = (x > in_2_val);
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
          | out_1[0] = in_2[0]->select(matching_ids);
        """.stripMargin

      val expr1 = reify { (a: (Float, Long)) =>
        var x = a._1.toInt % 3
        val y = {
          val z = 66
          if ({x += 1; x < z}) {
            10
          } else if (x > z) {
            -10
          } else {
            2
          }
        }
        x += y
        x > a._2
      }

      val genCode1 = CppTranspiler.transpileFilter(expr1)
      assertCodeEqualish(genCode1, output1)
    }

    "correctly optimize out transpiling Block Body where a constant literal is returned" in {
      val output1 =
        """
          | out_0[0] = in_1->clone();
          | out_1[0] = in_2->clone();
          | out_2[0] = in_3->clone();
        """.stripMargin

      val output2 =
        """
          | out_0[0] = nullable_bigint_vector::allocate();
          | out_0[0]->resize(0);
          | out_1[0] = nullable_float_vector::allocate();
          | out_1[0]->resize(0);
        """.stripMargin

      val expr1 = reify { (a: (Long, Float, Int)) =>
        val x = a._1 % 3 == 0
        var y = if (x) a._2 + 43 else a._2 - 92612
        y /= 10
        true
      }

      val expr2 = reify { (a: (Long, Float)) =>
        val x = if (a._1 % 3 == 0) 23 else 37
        val y = x * a._1
        false
      }

      val genCode1 = CppTranspiler.transpileFilter(expr1)
      val genCode2 = CppTranspiler.transpileFilter(expr2)
      assertCodeEqualish(genCode1, output1)
      assertCodeEqualish(genCode2, output2)
    }

    "correctly transpile filter operations involving `java.time.Instant`" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | int64_t in_1_val{};
          | int64_t in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |   bitmask[i] = (((in_1_val == 1648428244277340000) ? 0 : (in_1_val < 1648428244277340000) ? -1 : 1) < in_2_val);
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
          | out_1[0] = in_2[0]->select(matching_ids);
          """.stripMargin

      val output2 =
        """
          | size_t len = in_1[0]->count;
          | std::vector<size_t> bitmask(len);
          |
          | int64_t in_1_val{};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   bitmask[i] = (((1648428244277340000 == in_1_val) ? 0 : (1648428244277340000 < in_1_val) ? -1 : 1) != 0);
          | }
          |
          | auto matching_ids = cyclone::bitmask_to_matching_ids(bitmask);
          | out_0[0] = in_1[0]->select(matching_ids);
          """.stripMargin

      val genCode1 = CppTranspiler.transpileFilter(reify { (x: (Instant, Long)) => x._1.compareTo(Instant.parse("2022-03-28T00:44:04.277340Z")) < x._2 })
      val genCode2 = CppTranspiler.transpileFilter(reify { x: Instant => Instant.parse("2022-03-28T00:44:04.277340Z").compareTo(x) != 0 })
      assertCodeEqualish(genCode1, output1)
      assertCodeEqualish(genCode2, output2)
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
