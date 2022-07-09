package io.sparkcyclone.vectorengine

import io.sparkcyclone.native.code._
import io.sparkcyclone.native.code.CFunction2._
import io.sparkcyclone.native.code.VeScalarType
import io.sparkcyclone.spark.codegen.groupby.GroupByOutline

object SampleVeFunctions {
  final val DoublingFunction: CFunction2 = {
    val body = CodeLines.from(
      "*output = nullable_double_vector::allocate();",
      "output[0]->resize(input[0]->count);",
      "",
      CodeLines.forLoop("i", "input[0]->count") {
        List(
          "output[0]->data[i] = input[0]->data[i] * 2;",
          "output[0]->set_validity(i, input[0]->get_validity(i));"
        )
      }
    )

    CFunction2(
      "double_func",
      Seq(
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("input")),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("output"))
      ),
      body,
    )
  }

  final val FilterEvensFunction: CFunction2 = {
    val body = CodeLines.from(
      "*output1 = nullable_double_vector::allocate();",
      "*output2 = nullable_double_vector::allocate();",
      "",
      "auto *in0 = input0[0];",
      "auto *in1 = input1[0];",
      "auto *in2 = input2[0];",
      "auto *out1 = output1[0];",
      "auto *out2 = output2[0];",
      "",
      "out1->resize(in0->count);",
      "out2->resize(in0->count);",
      "",
      CodeLines.forLoop("i", "in0->count") {
        CodeLines.ifStatement("in0->data[i] % 2 == 0") {
          List(
            "out1->data[i] = in1->data[i];",
            "out2->data[i] = in2->data[i];",
            "out1->set_validity(i, in1->get_validity(i));",
            "out2->set_validity(i, in2->get_validity(i));"
          )
        }
      }
    )

    CFunction2(
      "filter_evens_func",
      Seq(
        CFunctionArgument.PointerPointer(VeNullableInt.makeCVector("input0")),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("input1")),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("input2")),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("output1")),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("output2"))
      ),
      body,
    )
  }

  final val PartitioningFunction: CFunction2 = {
    val body = CodeLines.from(
      "int SETS_TO_DO = 5;",
      "int MAX_SET_ID = SETS_TO_DO - 1;",
      "",
      "*output = static_cast<nullable_double_vector*>(malloc(sizeof(nullptr) * SETS_TO_DO));",
      "",
      CodeLines.forLoop("s", "SETS_TO_DO") {
        CodeLines.from(
          "sets[0] = s + 1;",
          "std::vector<double> nums;",
          "",
          CodeLines.forLoop("i", "input[0]->count") {
            CodeLines.from(
              "double v = input[0]->data[i];",
              CodeLines.ifStatement("(v >= s * 100 && v < (s + 1) * 100) || s == MAX_SET_ID") {
                "nums.push_back(v);"
              }
            )
          },
          GroupByOutline.scalarVectorFromStdVector(VeNullableDouble, "output[s]", "nums")
        )
      }
    )

    CFunction2(
      "partitioning_func",
      Seq(
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("input")),
        CFunctionArgument.Raw("int *sets"),
        CFunctionArgument.PointerPointer(VeNullableDouble.makeCVector("output"))
      ),
      body,
    )
  }
}
