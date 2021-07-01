package com.nec.spark.agile
import com.nec.spark.BenchTestingPossibilities.Testing.DataSize.SanityCheckSize
import com.nec.spark.SparkAdditions
import com.nec.spark.planning.simplesum.SimpleSumPlanTest.Source
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
object ExpressionGenerationSpec {}

final class ExpressionGenerationSpec extends AnyFreeSpec with BeforeAndAfter with SparkAdditions {
  import com.eed3si9n.expecty.Expecty.assert
  private implicit val nameCleaner: NameCleaner = NameCleaner.simple
  val cHeading =
    List(
      "extern \"C\" long f(non_null_double_vector* input_0, non_null_double_vector* output_0) {",
      "output_0->data = (double *)malloc(1 * sizeof(double));"
    )

  val cHeading2 =
    List(
      "extern \"C\" long f(non_null_double_vector* input_0, non_null_double_vector* output_0, non_null_double_vector* output_0) {",
      "output_0->data = (double *)malloc(1 * sizeof(double));",
      "output_0->data = (double *)malloc(1 * sizeof(double));"
    )

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

    assert(cGen(Seq(ref), Alias(null, "summy")() -> expr) == {
      cHeading ++ List(
        "double summy_accumulated = 0;",
        "for (int i = 0; i < input_0->count; i++) {",
        "summy_accumulated += input_0->data[i] - 1.0;",
        "}",
        "double summy_result = summy_accumulated;",
        "output_0->data[0] = summy_result;",
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

    assert(cGen(Seq(ref), Alias(null, "avy#123 + 51")() -> expr) == {
      cHeading ++ List(
        "double avy12351_accumulated = 0;",
        "int avy12351_counted = 0;",
        "for (int i = 0; i < input_0->count; i++) {",
        "avy12351_accumulated += input_0->data[i] - 1.0;",
        "avy12351_counted += 1;",
        "}",
        "double avy12351_result = avy12351_accumulated / avy12351_counted;",
        "output_0->data[0] = avy12351_result;",
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

    assert(cGen(Seq(ref), Alias(null, "avy#123 + 2")() -> expr) == {
      cHeading ++ List(
        "double avy1232_accumulated = 0;",
        "int avy1232_counted = 0;",
        "for (int i = 0; i < input_0->count; i++) {",
        "avy1232_accumulated += input_0->data[i] + 2.0;",
        "avy1232_counted += 1;",
        "}",
        "double avy1232_result = avy1232_accumulated / avy1232_counted;",
        "output_0->data[0] = avy1232_result;",
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

    assert(cGen(Seq(ref1, ref2), Alias(null, "avy#123 + avy#124")() -> expr) == {
      List(
        "extern \"C\" long f(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0) {",
        "output_0->data = (double *)malloc(1 * sizeof(double));",
        "double avy123avy124_accumulated = 0;",
        "int avy123avy124_counted = 0;",
        "for (int i = 0; i < input_0->count; i++) {",
        "avy123avy124_accumulated += input_0->data[i] + input_1->data[i];",
        "avy123avy124_counted += 1;",
        "}",
        "double avy123avy124_result = avy123avy124_accumulated / avy123avy124_counted;",
        "output_0->data[0] = avy123avy124_result;",
        "return 0;"
      )
    }.codeLines)
  }

  "Different expressions are found" - {
    List(
      "SELECT SUM(value) FROM nums",
      "SELECT SUM(value - 1) FROM nums",
      "SELECT AVG(2 * value) FROM nums",
      "SELECT AVG(2 * value), SUM(value) FROM nums",
      "SELECT AVG(2 * value), SUM(value - 1), value / 2 FROM nums GROUP BY (value / 2)"
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
        Source.CSV.generate(sparkSession, SanityCheckSize)
        import sparkSession.implicits._
        sparkSession.sql(sql).debugSqlHere
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
      cGen(Seq(ref), Alias(null, "summy")() -> expr, Alias(null, "avy#123 - 1.0")() -> expr2) ==
        List(
          """extern "C" long f(non_null_double_vector* input_0, non_null_double_vector* output_0, non_null_double_vector* output_1) {""",
          """output_0->data = (double *)malloc(1 * sizeof(double));""",
          """double summy_accumulated = 0;""",
          """output_1->data = (double *)malloc(1 * sizeof(double));""",
          """double avy12310_accumulated = 0;""",
          "int avy12310_counted = 0;",
          "for (int i = 0; i < input_0->count; i++) {",
          "summy_accumulated += input_0->data[i] - 1.0;",
          "avy12310_accumulated += input_0->data[i] - 1.0;",
          "avy12310_counted += 1;",
          "}",
          "double summy_result = summy_accumulated;",
          "output_0->data[0] = summy_result;",
          "double avy12310_result = avy12310_accumulated / avy12310_counted;",
          "output_1->data[0] = avy12310_result;",
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
        Seq(ref_value14, ref_value15),
        Seq(Alias(Add(ref_value14, ref_value15), "oot")())
      ) == List(
        """extern "C" long f(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0)""",
        "{",
        "output_0->count = input_0->count;",
        "output_0->data = (double*) malloc(output_0->count * sizeof(double));",
        "#pragma omp parallel for",
        "for (int i = 0; i < output_0->count; i++) {",
        "output_0->data[i] = input_0->data[i] + input_1->data[i];",
        "}",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Subtraction projection: value#14 - value#15" in {
    assert(
      cGenProject(
        Seq(ref_value14, ref_value15),
        Seq(Alias(Subtract(ref_value14, ref_value15), "oot")())
      ) == List(
        """extern "C" long f(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0)""",
        "{",
        "output_0->count = input_0->count;",
        "output_0->data = (double*) malloc(output_0->count * sizeof(double));",
        "#pragma omp parallel for",
        "for (int i = 0; i < output_0->count; i++) {",
        "output_0->data[i] = input_0->data[i] - input_1->data[i];",
        "}",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  "Multiple column projection: value#14 + value#15, value#14 - value#15" in {
    assert(
      cGenProject(
        Seq(ref_value14, ref_value15),
        Seq(
          Alias(Add(ref_value14, ref_value15), "oot")(),
          Alias(Subtract(ref_value14, ref_value15), "oot")()
        )
      ) == List(
        """extern "C" long f(non_null_double_vector* input_0, non_null_double_vector* input_1, non_null_double_vector* output_0, non_null_double_vector* output_1)""",
        "{",
        "output_0->count = input_0->count;",
        "output_0->data = (double*) malloc(output_0->count * sizeof(double));",
        "output_1->count = input_0->count;",
        "output_1->data = (double*) malloc(output_1->count * sizeof(double));",
        "#pragma omp parallel for",
        "for (int i = 0; i < output_0->count; i++) {",
        "output_0->data[i] = input_0->data[i] + input_1->data[i];",
        "output_1->data[i] = input_0->data[i] - input_1->data[i];",
        "}",
        "return 0;",
        "}"
      ).codeLines
    )
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def debugSqlHere: Dataset[T] = {
      info(dataSet.queryExecution.executedPlan.toString())
      dataSet
    }
  }

}
