package com.nec.native

import scala.reflect.runtime.universe._

final class TranspileMapUnitSpec extends CppTranspilerSpec {
  "CppTranspiler for Map Functions" should {
    "correctly transpile Int => Int" in {
      val gencode = CppTranspiler.transpileMap(reify( (x: Int) => x * 2 ))
      assert(supertrim(gencode.func.body.cCode).contains("in_1_val*2"))
    }

    "ensure proper order of arithmetic operations" in {
      val gencode = CppTranspiler.transpileMap(reify( (x: Int) => ((2 * x) + 12) - (x % 15)))
      assert(supertrim(gencode.func.body.cCode).contains("(((2*in_1_val)+12)-(in_1_val%15))"))
    }

    "correctly transpile Long => (Long, Long)" in {
      val output1 = """out_0[0] = in_1[0]->clone();"""
      val output2 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_bigint_vector::allocate();
          | out_0[0]->resize(len);
          | out_1[0] = nullable_bigint_vector::allocate();
          | out_1[0]->resize(len);
          |
          | int64_t in_1_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |
          |   out_0[0]->data[i] = in_1_val;
          |   out_1[0]->data[i] = (in_1_val * 2);
          |   out_0[0]->set_validity(i, 1);
          |   out_1[0]->set_validity(i, 1);
          | }
        """.stripMargin
      val output3 = "out_0[0] = in_2[0]->clone();"

      val genCode1 = CppTranspiler.transpileMap(reify { x: Long => x })
      val genCode2 = CppTranspiler.transpileMap(reify { x: Long => (x, x * 2) })
      val genCode3 = CppTranspiler.transpileMap(reify { x: (Long, Long) => x._2 })
      assertCodeEqualish(genCode1, output1)
      assertCodeEqualish(genCode2, output2)
      assertCodeEqualish(genCode3, output3)
    }

    "correctly transpile where primitive type conversions are included and the body is a Tree#Select" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_double_vector::allocate();
          | out_0[0]->resize(len);
          |
          | int64_t in_1_val {};
          | float in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   out_0[0]->data[i] = (double( (in_1_val + 2) ) * int32_t(int16_t( in_2_val )));
          |   out_0[0]->set_validity(i, 1);
          | }
          """.stripMargin

      val genCode1 = CppTranspiler.transpileMap(reify { x: (Long, Float) => (x._1 + 2).toDouble * x._2.toShort })
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile where primitive type conversions are included and the body is a Tree#Apply (i.e. Tuple output)" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_int_vector::allocate();
          | out_0[0]->resize(len);
          | out_1[0] = nullable_double_vector::allocate();
          | out_1[0]->resize(len);
          |
          | int64_t in_1_val {};
          | int64_t in_2_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   out_0[0]->data[i] = int32_t( (in_2_val / 2.0) );
          |   out_1[0]->data[i] = (float( (in_1_val + 2) ) * 3.14);
          |   out_0[0]->set_validity(i, 1);
          |   out_1[0]->set_validity(i, 1);
          | }
        """.stripMargin

      val genCode1 = CppTranspiler.transpileMap(reify { x: (Long, Long) => ((x._2 / 2.0).toInt, (x._1 + 2).toFloat * 3.14) })
      assertCodeEqualish(genCode1, output1)
    }
  }
}
