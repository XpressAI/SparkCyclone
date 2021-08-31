package com.nec.spark.agile

import com.nec.spark.SparkAdditions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.Metadata
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.agile.CExpressionEvaluation._
import com.nec.testing.SampleSource
import com.nec.testing.SampleSource.SampleColA
import com.nec.testing.Testing.DataSize.SanityCheckSize
object ExpressionGenerationSpec {}

final class ExpressionGenerationSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {

  def testFName: String = "test_f"
  private implicit val nameCleaner: NameCleaner = NameCleaner.simple
  "SUM((value#14 - 1.0)) is evaluated" in {
    val ref = AttributeReference(
      name = "value#14",
      dataType = DoubleType,
      nullable = false,
      metadata = Metadata.empty
    )()
    val expr = AggregateExpression(
      aggregateFunction = Sum(Subtract(ref, Literal(1.0, DoubleType))),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(cGen(testFName, Set("value#14"), Seq(ref), List(Alias(null, "summy")() -> expr)) == {
      List(
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* output_0_sum) {""",
        "output_0_sum->data = (double *)malloc(1 * sizeof(double));",
        "output_0_sum->count = 1;",
        "unsigned char* output_0_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
        "output_0_validity_buffer[0] = 0;",
        "double summy_accumulated = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < input_0->count; i++) {",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 && true)",
        "{",
        "output_0_validity_buffer[0] = 1;",
        "summy_accumulated += input_0->data[i] - 1.0;",
        "}",
        "}",
        "output_0_sum->data[0] = summy_accumulated;",
        "output_0_sum->validityBuffer = output_0_validity_buffer;",
        "return 0;"
      )
    }.codeLines)

  }

  "AVG((value#14 - 1.0)) is evaluated" in {
    val ref =
      AttributeReference(
        name = "abcd",
        dataType = DoubleType,
        nullable = false,
        metadata = Metadata.empty
      )()
    val expr = AggregateExpression(
      aggregateFunction = Average(Subtract(ref, Literal(1.0, DoubleType))),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(cGen(testFName, Set("abcd"), Seq(ref), List(Alias(null, "avy#123 + 51")() -> expr)) == {
      List(
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* output_0_average_sum, nullable_bigint_vector* output_0_average_count) {""",
        "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
        "output_0_average_sum->count = 1;",
        "output_0_average_count->data = (long *)malloc(1 * sizeof(long));",
        "output_0_average_count->count = 1;",
        "unsigned char* output_0_sum_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
        "unsigned char* output_0_count_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
        "output_0_sum_validity_buffer[0] = 0;",
        "output_0_count_validity_buffer[0] = 0;",
        "double avy12351_accumulated = 0;",
        "long avy12351_counted = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < input_0->count; i++) {",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 && true)",
        "{",
        "output_0_count_validity_buffer[0] = 1;",
        "output_0_sum_validity_buffer[0] = 1;",
        "avy12351_accumulated += input_0->data[i] - 1.0;",
        "avy12351_counted += 1;",
        "}",
        "}",
        "output_0_average_sum->data[0] = avy12351_accumulated;",
        "output_0_average_count->data[0] = avy12351_counted;",
        "output_0_average_count->validityBuffer = output_0_count_validity_buffer;",
        "output_0_average_sum->validityBuffer = output_0_sum_validity_buffer;",
        "return 0;"
      )
    }.codeLines)

  }
  "AVG((value#14 + 2.0)) is evaluated" in {
    val ref =
      AttributeReference(
        name = "abcd",
        dataType = DoubleType,
        nullable = false,
        metadata = Metadata.empty
      )()
    val expr = AggregateExpression(
      aggregateFunction = Average(Add(ref, Literal(2.0, DoubleType))),
      mode = Complete,
      isDistinct = false,
      filter = None
    )
    val a = cGen(testFName, Set("abcd"), Seq(ref), List(Alias(null, "avy#123 + 2")() -> expr))
    val b = List(
      s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* output_0_average_sum, nullable_bigint_vector* output_0_average_count) {""",
      "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
      "output_0_average_sum->count = 1;",
      "output_0_average_count->data = (long *)malloc(1 * sizeof(long));",
      "output_0_average_count->count = 1;",
      "unsigned char* output_0_sum_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
      "unsigned char* output_0_count_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
      "output_0_sum_validity_buffer[0] = 0;",
      "output_0_count_validity_buffer[0] = 0;",
      "double avy1232_accumulated = 0;",
      "long avy1232_counted = 0;",
      "#pragma _NEC ivdep",
      "for (int i = 0; i < input_0->count; i++) {",
      "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 &&  true)",
      "{",
      "output_0_validity_buffer[0] = 1;",
      "output_0_sum_validity_buffer[0] = 1;",
      "avy1232_accumulated += input_0->data[i] + 2.0;",
      "avy1232_counted += 1;",
      "}",
      "}",
      "output_0_average_sum->data[0] = avy1232_accumulated;",
      "output_0_average_count->data[0] = avy1232_counted;",
      "output_0_average_count->validityBuffer = output_0_count_validity_buffer;",
      "output_0_average_sum->validityBuffer = output_0_sum_validity_buffer;",
      "return 0;"
    )
    assert(cGen(testFName, Set("abcd"), Seq(ref), List(Alias(null, "avy#123 + 2")() -> expr)) == {
      List(
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* output_0_average_sum, nullable_bigint_vector* output_0_average_count) {""",
        "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
        "output_0_average_sum->count = 1;",
        "output_0_average_count->data = (long *)malloc(1 * sizeof(long));",
        "output_0_average_count->count = 1;",
        "unsigned char* output_0_sum_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
        "unsigned char* output_0_count_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
        "output_0_sum_validity_buffer[0] = 0;",
        "output_0_count_validity_buffer[0] = 0;",
        "double avy1232_accumulated = 0;",
        "long avy1232_counted = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < input_0->count; i++) {",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 &&  true)",
        "{",
        "output_0_count_validity_buffer[0] = 1;",
        "output_0_sum_validity_buffer[0] = 1;",
        "avy1232_accumulated += input_0->data[i] + 2.0;",
        "avy1232_counted += 1;",
        "}",
        "}",
        "output_0_average_sum->data[0] = avy1232_accumulated;",
        "output_0_average_count->data[0] = avy1232_counted;",
        "output_0_average_count->validityBuffer = output_0_count_validity_buffer;",
        "output_0_average_sum->validityBuffer = output_0_sum_validity_buffer;",
        "return 0;"
      )
    }.codeLines)
  }
  "AVG((value#14 + value#13)) is evaluated" in {
    val ref1 =
      AttributeReference(
        name = "abcd",
        dataType = DoubleType,
        nullable = false,
        metadata = Metadata.empty
      )()
    val ref2 =
      AttributeReference(
        name = "abcd_2",
        dataType = DoubleType,
        nullable = false,
        metadata = Metadata.empty
      )()
    val expr = AggregateExpression(
      aggregateFunction = Average(Add(ref1, ref2)),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(
      cGen(
        testFName,
        Set("abcd", "abcd_2"),
        Seq(ref1, ref2),
        List(Alias(null, "avy#123 + avy#124")() -> expr)
      ) == {
        List(
          s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* input_1, nullable_double_vector* output_0_average_sum, nullable_bigint_vector* output_0_average_count) {""",
          "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
          "output_0_average_sum->count = 1;",
          "output_0_average_count->data = (long *)malloc(1 * sizeof(long));",
          "output_0_average_count->count = 1;",
          "unsigned char* output_0_sum_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
          "unsigned char* output_0_count_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
          "output_0_sum_validity_buffer[0] = 0;",
          "output_0_count_validity_buffer[0] = 0;",
          "double avy123avy124_accumulated = 0;",
          "long avy123avy124_counted = 0;",
          "#pragma _NEC ivdep",
          "for (int i = 0; i < input_0->count; i++) {",
          "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 &&  ((input_1->validityBuffer[i/8] >> i % 8) & 0x1) == 1)",
          "{",
          "output_0_count_validity_buffer[0] = 1;",
          "output_0_sum_validity_buffer[0] = 1;",
          "avy123avy124_accumulated += input_0->data[i] + input_1->data[i];",
          "avy123avy124_counted += 1;",
          "}",
          "}",
          "output_0_average_sum->data[0] = avy123avy124_accumulated;",
          "output_0_average_count->data[0] = avy123avy124_counted;",
          "output_0_average_count->validityBuffer = output_0_count_validity_buffer;",
          "output_0_average_sum->validityBuffer = output_0_sum_validity_buffer;",
          "return 0;"
        )
      }.codeLines
    )
  }

  "Different expressions are found" - {
    List(
      s"SELECT SUM(${SampleColA}) FROM nums",
      s"SELECT SUM(${SampleColA} - 1) FROM nums",
      s"SELECT AVG(2 * ${SampleColA}) FROM nums",
      s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA}) FROM nums",
      s"SELECT AVG(2 * ${SampleColA}), SUM(${SampleColA} - 1), ${SampleColA} / 2 FROM nums GROUP BY (${SampleColA} / 2)"
    ).take(2).foreach { sql =>
      s"$sql" in withSparkSession2(
        _.config(CODEGEN_FALLBACK.key, value = false)
          .config("spark.sql.codegen.comments", value = true)
          .withExtensions(sse =>
            sse.injectPlannerStrategy(sparkSession =>
              new Strategy {
                override def apply(plan: LogicalPlan): Seq[SparkPlan] =
                  plan match {
                    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
                      info(s"Grouping ==> ${groupingExpressions.mkString}")
                      info(s"Result ==> ${resultExpressions.mkString}")
                      info(s"Result ==> ${resultExpressions.collect {
                        case Alias(ae @ AggregateExpression(Sum(sth), mode, isDistinct, filter, resultId), name) =>
                          info(sth.toString)
                          info(sth.getClass.getCanonicalName)
                          ae.productIterator.mkString("|")
                      }.mkString}")
                      Nil
                    case _ => Nil
                  }
              }
            )
          )
      ) { sparkSession =>
        SampleSource.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._
        sparkSession.sql(sql).debugSqlHere { ds =>
          ds.collect()
        }
      }
    }
  }

  "SUM((value#14 - 1.0)), AVG((value#14 - 1.0)) is evaluated" in {
    val ref =
      AttributeReference(
        name = "abcd",
        dataType = DoubleType,
        nullable = false,
        metadata = Metadata.empty
      )()

    val expr = AggregateExpression(
      aggregateFunction = Sum(Subtract(ref, Literal(1.0, DoubleType))),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    val expr2 = AggregateExpression(
      aggregateFunction = Average(Subtract(ref, Literal(1.0, DoubleType))),
      mode = Complete,
      isDistinct = false,
      filter = None
    )

    assert(
      cGen(
        testFName,
        Set("abcd"),
        Seq(ref),
        List(Alias(null, "summy")() -> expr, Alias(null, "avy#123 - 1.0")() -> expr2)
      ) ==
        List(
          s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* output_0_sum, nullable_double_vector* output_1_average_sum, nullable_bigint_vector* output_1_average_count) {""",
          "output_0_sum->data = (double *)malloc(1 * sizeof(double));",
          "output_0_sum->count = 1;",
          "unsigned char* output_0_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
          "output_0_validity_buffer[0] = 0;",
          "double summy_accumulated = 0;",
          "output_1_average_sum->data = (double *)malloc(1 * sizeof(double));",
          "output_1_average_sum->count = 1;",
          "output_1_average_count->data = (long *)malloc(1 * sizeof(long));",
          "output_1_average_count->count = 1;",
          "unsigned char* output_1_sum_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
          "unsigned char* output_1_count_validity_buffer = (unsigned char *) malloc(1 * sizeof(unsigned char));",
          "output_1_sum_validity_buffer[0] = 0;",
          "output_1_count_validity_buffer[0] = 0;",
          "double avy12310_accumulated = 0;",
          "long avy12310_counted = 0;",
          "#pragma _NEC ivdep",
          "for (int i = 0; i < input_0->count; i++) {",
          "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 && true)",
          "{",
          "output_0_validity_buffer[0] = 1;",
          "summy_accumulated += input_0->data[i] - 1.0;",
          "}",
          "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 && true)",
          "{",
          "output_1_count_validity_buffer[0] = 1;",
          "output_1_sum_validity_buffer[0] = 1;",
          "avy12310_accumulated += input_0->data[i] - 1.0;",
          "avy12310_counted += 1;",
          "}",
          "}",
          "output_0_sum->data[0] = summy_accumulated;",
          "output_0_sum->validityBuffer = output_0_validity_buffer;",
          "output_1_average_sum->data[0] = avy12310_accumulated;",
          "output_1_average_count->data[0] = avy12310_counted;",
          "output_1_average_count->validityBuffer = output_1_count_validity_buffer;",
          "output_1_average_sum->validityBuffer = output_1_sum_validity_buffer;",
          "return 0;"
        ).codeLines
    )
  }

  private val ref_value14 =
    AttributeReference(
      name = "value#14",
      dataType = DoubleType,
      nullable = false,
      metadata = Metadata.empty
    )()
  private val ref_value15 =
    AttributeReference(
      name = "value#15",
      dataType = DoubleType,
      nullable = false,
      metadata = Metadata.empty
    )()

  private val ref_value16 =
    AttributeReference(
      name = "value#16",
      dataType = DoubleType,
      nullable = false,
      metadata = Metadata.empty
    )()
  private val ref_value17 =
    AttributeReference(
      name = "value#17",
      dataType = DoubleType,
      nullable = false,
      metadata = Metadata.empty
    )()

  "Addition projection: value#14 + value#15" in {
    assert(
      cGenProject(
        fName = testFName,
        inputReferences = Set("value#14", "value#15"),
        childOutputs = Seq(ref_value14, ref_value15),
        resultExpressions = Seq(Alias(Add(ref_value14, ref_value15), "oot")()),
        maybeFilter = None
      ) == List(
        "#include <cmath>",
        "#include <bitset>",
        "#include <iostream>",
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* input_1, nullable_double_vector* output_0)""",
        "{",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "unsigned char *validity_buffer_0 = (unsigned char*) malloc(ceil(output_0_count/8.0) * sizeof(unsigned char));",
        "std::bitset<8> validity_bitset_0;",
        "int j_0 = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 &&  ((input_1->validityBuffer[i/8] >> i % 8) & 0x1) == 1)",
        "{",
        "output_0_data[i] = input_0->data[i] + input_1->data[i];",
        "validity_bitset_0.set(i%8, true);",
        "}",
        "else { validity_bitset_0.set(i%8, false);}",
        "if(i % 8 == 7 || i == output_0_count - 1) { ",
        "validity_buffer_0[j_0] = (static_cast<unsigned char>(validity_bitset_0.to_ulong()));",
        "j_0 += 1;",
        "validity_bitset_0.reset(); }",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "output_0->validityBuffer = validity_buffer_0;",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Sorting" in {
    assert(
      cGenSort(testFName, Seq(ref_value14, ref_value15), ref_value14) == List(
        "#include \"frovedis/core/radix_sort.hpp\"",
        "#include <tuple>",
        "#include <bitset>",
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* input_1, nullable_double_vector* output_0, nullable_double_vector* output_1)""",
        "{",
        "std::tuple<int, int>* sort_column_validity_buffer = (std::tuple<int, int> *) malloc(input_0->count * sizeof(std::tuple<int, double>));",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "unsigned char *validity_buffer_0 = (unsigned char*) malloc(ceil(output_0_count/8.0) * sizeof(unsigned char));",
        "std::bitset<8> validity_bitset_0;",
        "int j_0 = 0;",
        "long output_1_count = input_0->count;",
        "double *output_1_data = (double*) malloc(output_1_count * sizeof(double));",
        "unsigned char *validity_buffer_1 = (unsigned char*) malloc(ceil(output_1_count/8.0) * sizeof(unsigned char));",
        "std::bitset<8> validity_bitset_1;",
        "int j_1 = 0;",
        "for(int i = 0; i < input_0->count; i++)",
        "{",
        "sort_column_validity_buffer[i] = std::tuple<int, double>{((input_0->validityBuffer[i/8] >> i % 8) & 0x1), i};",
        "}",
        "frovedis::radix_sort(input_0->data, sort_column_validity_buffer, input_0->count);",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "if(std::get<0>(sort_column_validity_buffer[i]) == 1) {",
        "validity_bitset_0.set(i%8, true);",
        "output_0_data[i] = input_0->data[i];",
        "} else {",
        "validity_bitset_0.set(i%8, false);}",
        "if(i % 8 == 7 || i == output_0_count - 1) { ",
        "validity_buffer_0[j_0] = (static_cast<unsigned char>(validity_bitset_0.to_ulong()));",
        "j_0 += 1;",
        "validity_bitset_0.reset(); }",
        "if((input_1->validityBuffer[std::get<1>(sort_column_validity_buffer[i])/8] >> (std::get<1>(sort_column_validity_buffer[i]) % 8) & 0x1) == 1) {",
        "validity_bitset_1.set(i%8, true);",
        "output_1_data[i] = input_1->data[std::get<1>(sort_column_validity_buffer[i])];",
        "} else {",
        "validity_bitset_1.set(i%8, false);}",
        "if(i % 8 == 7 || i == output_0_count - 1) { ",
        "validity_buffer_1[j_1] = (static_cast<unsigned char>(validity_bitset_1.to_ulong()));",
        "j_1 += 1;",
        "validity_bitset_1.reset(); }",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "output_0->validityBuffer = validity_buffer_0;",
        "output_1->count = output_1_count;",
        "output_1->data = output_1_data;",
        "output_1->validityBuffer = validity_buffer_1;",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Joining" in {
    assert(
      cGenJoin(
        testFName,
        Seq(ref_value14, ref_value15, ref_value16, ref_value17),
        Seq(ref_value14, ref_value15),
        Seq(ref_value14, ref_value16).map(_.exprId).toSet,
        Seq(ref_value15, ref_value17).map(_.exprId).toSet,
        ref_value16,
        ref_value17
      ) == List(
        "#include \"frovedis/dataframe/join.hpp\"",
        "#include <cmath>",
        "#include <bitset>",
        "#include \"frovedis/dataframe/join.cc\"",
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* input_1, nullable_double_vector* input_2, nullable_double_vector* input_3, nullable_double_vector* output_0, nullable_double_vector* output_1)""",
        "{",
        "std::vector <double> left_vec;",
        "std::vector<size_t> left_idx;",
        "std::vector <double> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        "for(int i = 0; i < input_0->count; i++) { ",
        "left_vec.push_back(input_2->data[i]);",
        "left_idx.push_back(i);",
        "right_vec.push_back(input_3->data[i]);",
        "right_idx.push_back(i);",
        "}",
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        "frovedis::equi_join<double>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);",
        "long validityBuffSize = ceil(left_out.size() / 8.0);",
        "output_0->data=(double *) malloc(left_out.size() * sizeof(double));",
        "output_0->validityBuffer=(unsigned char *) malloc(validityBuffSize * sizeof(unsigned char*));",
        "std::bitset<8> validity_bitset_0;",
        "int j_0 = 0;",
        "output_1->data=(double *) malloc(left_out.size() * sizeof(double));",
        "output_1->validityBuffer=(unsigned char *) malloc(validityBuffSize * sizeof(unsigned char*));",
        "std::bitset<8> validity_bitset_1;",
        "int j_1 = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < left_out.size(); i++) {",
        "if(((input_0->validityBuffer[left_out[i]/8] >> left_out[i] % 8) & 0x1) == 1) {",
        "validity_bitset_0.set(i%8, true);",
        "output_0->data[i] = input_0->data[left_out[i]];",
        "} else {",
        "validity_bitset_0.set(i%8, false);",
        "}",
        "if(i % 8 == 7 || i == left_out.size() - 1) { ",
        "output_0->validityBuffer[j_0] = (static_cast<unsigned char>(validity_bitset_0.to_ulong()));",
        "j_0 += 1;",
        "validity_bitset_0.reset(); }",
        "if(((input_1->validityBuffer[right_out[i]/8] >> right_out[i] % 8) & 0x1) == 1) {",
        "validity_bitset_1.set(i%8, true);",
        "output_1->data[i] = input_1->data[right_out[i]];",
        "} else {",
        "validity_bitset_1.set(i%8, false);",
        "}",
        "if(i % 8 == 7 || i == left_out.size() - 1) { ",
        "output_1->validityBuffer[j_1] = (static_cast<unsigned char>(validity_bitset_1.to_ulong()));",
        "j_1 += 1;",
        "validity_bitset_1.reset(); }",
        "}",
        "output_0->count = left_out.size();",
        "output_1->count = left_out.size();",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Subtraction projection: value#14 - value#15" in {
    assert(
      cGenProject(
        fName = testFName,
        inputReferences = Set("value#14", "value#15"),
        childOutputs = Seq(ref_value14, ref_value15),
        resultExpressions = Seq(Alias(Subtract(ref_value14, ref_value15), "oot")()),
        maybeFilter = None
      ) == List(
        "#include <cmath>",
        "#include <bitset>",
        "#include <iostream>",
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* input_1, nullable_double_vector* output_0)""",
        "{",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "unsigned char *validity_buffer_0 = (unsigned char*) malloc(ceil(output_0_count/8.0) * sizeof(unsigned char));",
        "std::bitset<8> validity_bitset_0;",
        "int j_0 = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 && ((input_1->validityBuffer[i/8] >> i % 8) & 0x1) == 1)",
        "{",
        "output_0_data[i] = input_0->data[i] - input_1->data[i];",
        "validity_bitset_0.set(i%8, true);",
        "}",
        "else { validity_bitset_0.set(i%8, false);}",
        "if(i % 8 == 7 || i == output_0_count - 1) { ",
        "validity_buffer_0[j_0] = (static_cast<unsigned char>(validity_bitset_0.to_ulong()));",
        "j_0 += 1;",
        "validity_bitset_0.reset(); }",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "output_0->validityBuffer = validity_buffer_0;",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Multiple column projection: value#14 + value#15, value#14 - value#15" in {
    assert(
      cGenProject(
        fName = testFName,
        inputReferences = Set("value#14", "value#15"),
        childOutputs = Seq(ref_value14, ref_value15),
        resultExpressions = Seq(
          Alias(Add(ref_value14, ref_value15), "oot")(),
          Alias(Subtract(ref_value14, ref_value15), "oot")()
        ),
        maybeFilter = None
      ) == List(
        "#include <cmath>",
        "#include <bitset>",
        "#include <iostream>",
        s"""extern "C" long ${testFName}(nullable_double_vector* input_0, nullable_double_vector* input_1, nullable_double_vector* output_0, nullable_double_vector* output_1)""",
        "{",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "unsigned char *validity_buffer_0 = (unsigned char*) malloc(ceil(output_0_count/8.0) * sizeof(unsigned char));",
        "std::bitset<8> validity_bitset_0;",
        "int j_0 = 0;",
        "long output_1_count = input_0->count;",
        "double *output_1_data = (double*) malloc(output_1_count * sizeof(double));",
        "unsigned char *validity_buffer_1 = (unsigned char*) malloc(ceil(output_1_count/8.0) * sizeof(unsigned char));",
        "std::bitset<8> validity_bitset_1;",
        "int j_1 = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 &&  ((input_1->validityBuffer[i/8] >> i % 8) & 0x1) == 1)",
        "{",
        "output_0_data[i] = input_0->data[i] + input_1->data[i];",
        "validity_bitset_0.set(i%8, true);",
        "}",
        "else { validity_bitset_0.set(i%8, false);}",
        "if(i % 8 == 7 || i == output_0_count - 1) { ",
        "validity_buffer_0[j_0] = (static_cast<unsigned char>(validity_bitset_0.to_ulong()));",
        "j_0 += 1;",
        "validity_bitset_0.reset(); }",
        "if(((input_0->validityBuffer[i/8] >> i % 8) & 0x1) == 1 && ((input_1->validityBuffer[i/8] >> i % 8) & 0x1) == 1)",
        "{",
        "output_1_data[i] = input_0->data[i] - input_1->data[i];",
        "validity_bitset_1.set(i%8, true);",
        "}",
        "else { validity_bitset_1.set(i%8, false);}",
        "if(i % 8 == 7 || i == output_0_count - 1) { ",
        "validity_buffer_1[j_1] = (static_cast<unsigned char>(validity_bitset_1.to_ulong()));",
        "j_1 += 1;",
        "validity_bitset_1.reset(); }",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "output_0->validityBuffer = validity_buffer_0;",
        "output_1->count = output_1_count;",
        "output_1->data = output_1_data;",
        "output_1->validityBuffer = validity_buffer_1;",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere[V](f: Dataset[T] => V): V = {
      withClue(dataSet.queryExecution.executedPlan.toString()) {
        f(dataSet)
      }
    }
  }

}
