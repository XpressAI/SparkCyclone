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
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* output_0_sum) {""",
        "output_0_sum->data = (double *)malloc(1 * sizeof(double));",
        "output_0_sum->count = 1;",
        "double summy_accumulated = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < input_0->count; i++) {",
        "summy_accumulated += input_0->data[i] - 1.0;",
        "}",
        "output_0_sum->data[0] = summy_accumulated;",
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
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* output_0_average_sum, non_null_double_vector* output_0_average_count) {""",
        "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
        "output_0_average_sum->count = 1;",
        "output_0_average_count->data = (double *)malloc(1 * sizeof(double));",
        "output_0_average_count->count = 1;",
        "double avy12351_accumulated = 0;",
        "int avy12351_counted = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < input_0->count; i++) {",
        "avy12351_accumulated += input_0->data[i] - 1.0;",
        "}",
        "output_0_average_sum->data[0] = avy12351_accumulated;",
        "output_0_average_count->data[0] = input_0->count;",
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

    assert(cGen(testFName, Set("abcd"), Seq(ref), List(Alias(null, "avy#123 + 2")() -> expr)) == {
      List(
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* output_0_average_sum, non_null_double_vector* output_0_average_count) {""",
        "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
        "output_0_average_sum->count = 1;",
        "output_0_average_count->data = (double *)malloc(1 * sizeof(double));",
        "output_0_average_count->count = 1;",
        "double avy1232_accumulated = 0;",
        "int avy1232_counted = 0;",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < input_0->count; i++) {",
        "avy1232_accumulated += input_0->data[i] + 2.0;",
        "}",
        "output_0_average_sum->data[0] = avy1232_accumulated;",
        "output_0_average_count->data[0] = input_0->count;",
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
          s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0_average_sum, non_null_double_vector* output_0_average_count) {""",
          "output_0_average_sum->data = (double *)malloc(1 * sizeof(double));",
          "output_0_average_sum->count = 1;",
          "output_0_average_count->data = (double *)malloc(1 * sizeof(double));",
          "output_0_average_count->count = 1;",
          "double avy123avy124_accumulated = 0;",
          "int avy123avy124_counted = 0;",
          "#pragma _NEC ivdep",
          "for (int i = 0; i < input_0->count; i++) {",
          "avy123avy124_accumulated += input_0->data[i] + input_1->data[i];",
          "}",
          "output_0_average_sum->data[0] = avy123avy124_accumulated;",
          "output_0_average_count->data[0] = input_0->count;",
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
          s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* output_0_sum, non_null_double_vector* output_1_average_sum, non_null_double_vector* output_1_average_count) {""",
          "output_0_sum->data = (double *)malloc(1 * sizeof(double));",
          "output_0_sum->count = 1;",
          "double summy_accumulated = 0;",
          "output_1_average_sum->data = (double *)malloc(1 * sizeof(double));",
          "output_1_average_sum->count = 1;",
          "output_1_average_count->data = (double *)malloc(1 * sizeof(double));",
          "output_1_average_count->count = 1;",
          "double avy12310_accumulated = 0;",
          "int avy12310_counted = 0;",
          "#pragma _NEC ivdep",
          "for (int i = 0; i < input_0->count; i++) {",
          "summy_accumulated += input_0->data[i] - 1.0;",
          "avy12310_accumulated += input_0->data[i] - 1.0;",
          "}",
          "output_0_sum->data[0] = summy_accumulated;",
          "output_1_average_sum->data[0] = avy12310_accumulated;",
          "output_1_average_count->data[0] = input_0->count;",
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

  "Addition projection: value#14 + value#15" in {
    assert(
      cGenProject(
        testFName,
        Set("value#14", "value#15"),
        Seq(ref_value14, ref_value15),
        Seq(Alias(Add(ref_value14, ref_value15), "oot")())
      ) == List(
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0)""",
        "{",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "output_0_data[i] = input_0->data[i] + input_1->data[i];",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Sorting" in {
    assert(
      cGenSort(testFName, Seq(ref_value14, ref_value15), ref_value14) == List(
        "#include \"frovedis/core/radix_sort.hpp\"",
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0, non_null_double_vector* output_1)""",
        "{",
        "int* indices = (int *) malloc(input_0.count * sizeof(int));",
        "for(int i = 0; i < input_0.count; i++)",
        "{",
        "indices[i] = i;",
        "}",
        "frovedis::radix_sort(indices, input_0.data, input_0.count);",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "long output_1_count = input_0->count;",
        "double *output_1_data = (double*) malloc(output_1_count * sizeof(double));",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "output_0_data[indices[i]] = input_0->data[i];",
        "output_1_data[indices[i]] = input_1->data[i];",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "output_1->count = output_1_count;",
        "output_1->data = output_1_data;",
        "return 0;",
        "}"
      ).codeLines
    )
  }
  "Subtraction projection: value#14 - value#15" in {
    assert(
      cGenProject(
        testFName,
        Set("value#14", "value#15"),
        Seq(ref_value14, ref_value15),
        Seq(Alias(Subtract(ref_value14, ref_value15), "oot")())
      ) == List(
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0)""",
        "{",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "output_0_data[i] = input_0->data[i] - input_1->data[i];",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Multiple column projection: value#14 + value#15, value#14 - value#15" in {
    assert(
      cGenProject(
        testFName,
        Set("value#14", "value#15"),
        Seq(ref_value14, ref_value15),
        Seq(
          Alias(Add(ref_value14, ref_value15), "oot")(),
          Alias(Subtract(ref_value14, ref_value15), "oot")()
        )
      ) == List(
        s"""extern "C" long ${testFName}(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0, non_null_double_vector* output_1)""",
        "{",
        "long output_0_count = input_0->count;",
        "double *output_0_data = (double*) malloc(output_0_count * sizeof(double));",
        "long output_1_count = input_0->count;",
        "double *output_1_data = (double*) malloc(output_1_count * sizeof(double));",
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {",
        "output_0_data[i] = input_0->data[i] + input_1->data[i];",
        "output_1_data[i] = input_0->data[i] - input_1->data[i];",
        "}",
        "output_0->count = output_0_count;",
        "output_0->data = output_0_data;",
        "output_1->count = output_1_count;",
        "output_1->data = output_1_data;",
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
