package com.nec.native

import scala.reflect.runtime.universe._

final class TranspileMapUnitSpec extends CppTranspilerSpec {
  "CppTranspiler for Map Functions" should {
    "correctly transpile a set of basic arithmetic operations while preserving the order of operations" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_int_vector::allocate();
          | out_0[0]->resize(len);
          |
          | int32_t in_1_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |
          |
          |   out_0[0]->data[i] = (((2 * in_1_val) + 12) - (in_1_val % 15));
          |   out_0[0]->set_validity(i, 1);
          | }
        """.stripMargin

      val genCode1 = CppTranspiler.transpileMap(reify( (x: Int) => ((2 * x) + 12) - (x % 15)))
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile an Ident Body for both the non-Tuple and Tuple input cases" in {
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

    "correctly transpile a Literal Constant Body" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          | out_0[0] = nullable_double_vector::constant(len, 2.71);
        """.stripMargin

      val genCode1 = CppTranspiler.transpileMap(reify { (a: Int) => 2.71 })
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile a Select Body with primitive type conversions" in {
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

    "correctly transpile an Apply Body (Tuple) with primitive type conversions" in {
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

    "correctly transpile an If Body" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_int_vector::allocate();
          | out_0[0]->resize(len);
          |
          | int32_t in_1_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |
          |   out_0[0]->data[i] = (((in_1_val % 2) == 0)) ? ((in_1_val + 43)) : ((in_1_val - 92612));
          |   out_0[0]->set_validity(i, 1);
          | }
        """.stripMargin

      val genCode1 = CppTranspiler.transpileMap(reify { (a: Int) => if (a % 2 == 0) a + 43 else a - 92612 })
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile a Block Body with ValDefs, Assigns, and Ifs" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_float_vector::allocate();
          | out_0[0]->resize(len);
          |
          | int64_t in_1_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |
          |   const float w = float(3.14);
          |   int32_t x = ((in_1_val % 3) == 0);
          |   int64_t y = 12;
          |
          |   out_0[0]->data[i] = (
          |     ({
          |       y = (y + in_1_val);
          |       x;
          |     })
          |   ) ? (
          |     ({
          |       const int32_t z = 10;
          |       float( (z + 11) );
          |     })
          |   ) : (
          |     ((in_1_val == 10)) ?
          |       ((float(y) * float(3.14))) :
          |       ((45.0 + w))
          |   );
          |   out_0[0]->set_validity(i, 1);
          | }
        """.stripMargin

      val expr = reify { (a: Long) =>
        val w = 3.14.toFloat
        var x = a % 3 == 0
        var y = 12l
        if ({ y += a; x }) {
          val z = 10
          (z + 11).toFloat
        } else if (a == 10) {
          y.toFloat * 3.14.toFloat
        } else {
          45f + w
        }
      }

      val genCode1 = CppTranspiler.transpileMap(expr)
      assertCodeEqualish(genCode1, output1)
    }

    "correctly transpile a Block Body with Ifs inside Tuple" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          |
          | out_0[0] = nullable_double_vector::allocate();
          | out_0[0]->resize(len);
          | out_1[0] = nullable_double_vector::allocate();
          | out_1[0]->resize(len);
          |
          | int64_t in_1_val {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |
          |   const int32_t x = ((in_1_val % 3) == 0);
          |   double y = 12.0;
          |   double z = 2.71;
          |
          |   out_0[0]->data[i] = (
          |     ({
          |       y = (y + in_1_val);
          |       x;
          |     })
          |   ) ? ((z + 11)) : ((y / 3.14));
          |
          |   out_1[0]->data[i] = ({
          |     const double zz = (y + 2);
          |     (zz * 4);
          |   });
          |
          |   out_0[0]->set_validity(i, 1);
          |   out_1[0]->set_validity(i, 1);
          | }
        """.stripMargin

      val expr = reify { (a: Long) =>
        val x = a % 3 == 0
        var y = 12.0
        var z = 2.71
        (
          if ({ y += a; x }) {
            (z + 11)
          } else {
            y / 3.14
          },
          {
            val zz = y + 2
            zz * 4
          }
        )
      }

      val genCode1 = CppTranspiler.transpileMap(expr)
      assertCodeEqualish(genCode1, output1)
    }

    "correctly optimize out transpiling Block Body where a constant literal is returned" in {
      val output1 =
        """
          | size_t len = in_1[0]->count;
          | out_0[0] = nullable_double_vector::constant(len, 3.14);
        """.stripMargin

      val expr = reify { (a: Long) =>
        val x = a % 3 == 0
        var y = if (x) a + 43 else a - 92612
        y /= 10
        3.14
      }
      val genCode1 = CppTranspiler.transpileMap(expr)
      assertCodeEqualish(genCode1, output1)
    }
  }
}
