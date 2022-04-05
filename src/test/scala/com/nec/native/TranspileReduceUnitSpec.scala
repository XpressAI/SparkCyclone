package com.nec.native

import scala.reflect.runtime.universe._

final class TranspileReduceUnitSpec extends CppTranspilerSpec {
  "CppTranspiler for Reduce Functions" should {
    "correctly transpile a reduction of Long, Long => Long" in {
      val expected =
        """
          | size_t len = in_1[0]->count;
          | out_0[0] = nullable_bigint_vector::allocate();
          | out_0[0]->resize(1);
          |
          | int64_t in_1_val {};
          | int64_t agg_1 {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   agg_1 = (in_1_val + agg_1);
          | }
          |
          | out_0[0]->data[0] = agg_1;
          | out_0[0]->set_validity(0, 1);
        """.stripMargin

      val groupByCode = CppTranspiler.transpileReduce(reify({ (x: Long, y: Long) => x + y }))
      assertCodeEqualish(groupByCode, expected)
    }

    "correctly transpile a reduction of (Long, Long), (Long, Long) => Long" in {
      val expected =
        """
          | size_t len = in_1[0]->count;
          | out_0[0] = nullable_bigint_vector::allocate();
          | out_0[0]->resize(1);
          | out_1[0] = nullable_bigint_vector::allocate();
          | out_1[0]->resize(1);
          |
          | int64_t in_1_val {};
          | int64_t in_2_val {};
          | int64_t agg_1 {};
          | int64_t agg_2 {};
          |
          | for (auto i = 0; i < len; i++) {
          |   in_1_val = in_1[0]->data[i];
          |   in_2_val = in_2[0]->data[i];
          |
          |   agg_1 = (in_1_val + agg_2);
          |   agg_2 = (in_2_val + agg_1);
          | }
          |
          | out_0[0]->data[0] = agg_1;
          | out_0[0]->set_validity(0, 1);
          | out_1[0]->data[0] = agg_2;
          | out_1[0]->set_validity(0, 1);
          """.stripMargin

      val groupByCode = CppTranspiler.transpileReduce(reify({ (x: (Long, Long), y: (Long, Long)) => (x._1 + y._2, x._2 + y._1) }))
      assertCodeEqualish(groupByCode, expected)
    }
  }
}
