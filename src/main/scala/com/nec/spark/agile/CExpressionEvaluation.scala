package com.nec.spark.agile
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.Divide
import org.apache.spark.sql.catalyst.expressions.Abs
import org.apache.spark.sql.catalyst.expressions.aggregate.Corr
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Max

object CExpressionEvaluation {
  def cGenProject(fName: String, inputs: Seq[Attribute], resultExpressions: Seq[NamedExpression])(implicit
    nameCleaner: NameCleaner
  ): CodeLines = {
    val inputBits = inputs.zipWithIndex
      .map { case (i, idx) =>
        s"non_null_double_vector* input_${idx}"
      }

    val outputBits = resultExpressions.zipWithIndex.map { case (i, idx) =>
      s"non_null_double_vector* output_${idx}"
    }

    val arguments = inputBits ++ outputBits

    List[List[String]](
      List(s"""extern "C" long ${fName}(${arguments.mkString(", ")})""", "{"),
      resultExpressions.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"long output_${idx}_count = input_0->count;",
          s"double *output_${idx}_data = (double*) malloc(output_${idx}_count * sizeof(double));"
        )
      }.toList,
      List(
        "#pragma _NEC ivdep",
        "for (int i = 0; i < output_0_count; i++) {"),
        resultExpressions.zipWithIndex.flatMap { case (re, idx) =>
          List(s"output_${idx}_data[i] = ${evaluateExpression(inputs, re)};")
        }.toList,
      List("}"),
      // Set outputs
      resultExpressions.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"output_${idx}->count = output_${idx}_count;",
          s"output_${idx}->data = output_${idx}_data;"
        )
      }.toList,

      List(
        "return 0;", 
        "}"
      )
    ).flatten.codeLines
  }

  final case class AggregateDescription(
    init: List[String],
    iter: List[String],
    result: List[String],
    outputArguments: List[String]
  )

  def evaluateExpression(input: Seq[Attribute], expression: Expression): String = {
    expression match {
      case alias @ Alias(expr, name) => evaluateSub(input, alias.child)
      case NamedExpression(name, DoubleType | IntegerType) =>
        input.indexWhere(_.name == name) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx => s"input_${idx}->data[i]"
        }
    }
  }

  def evaluateSub(inputs: Seq[Attribute], expression: Expression): String = {
    expression match {
      case AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.name == name) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx =>
            s"input_${idx}->data[i]"
        }
      case Subtract(left, right, _) =>
        s"${evaluateSub(inputs, left)} - ${evaluateSub(inputs, right)}"
      case Multiply(left, right, _) =>
        s"${evaluateSub(inputs, left)} * ${evaluateSub(inputs, right)}"
      case Add(left, right, _) =>
        s"${evaluateSub(inputs, left)} + ${evaluateSub(inputs, right)}"
      case Divide(left, right, _) => 
        s"${evaluateSub(inputs, left)} / ${evaluateSub(inputs, right)}"
      case Abs(v) =>
        s"abs(${evaluateSub(inputs, v)})"
      case Literal(v, DoubleType | IntegerType) =>
        s"$v"
    }
  }

  def process(
    inputs: Seq[Attribute],
    cleanName: String,
    aggregateExpression: AggregateExpression,
    idx: Int
  ): Option[AggregateDescription] = {
    PartialFunction.condOpt(aggregateExpression.aggregateFunction) {
      case Sum(sub) =>
        AggregateDescription(
          init = List(
            s"output_${idx}_sum->data = (double *)malloc(1 * sizeof(double));",
            s"output_${idx}_sum->count = 1;",
            s"double ${cleanName}_accumulated = 0;"
          ),
          iter = List(s"${cleanName}_accumulated += ${evaluateSub(inputs, sub)};"),
          result = List(s"output_${idx}_sum->data[0] = ${cleanName}_accumulated;"),
          outputArguments = List(s"non_null_double_vector* output_${idx}_sum")
        )
      case Average(sub) =>
        val outputSum = s"output_${idx}_average_sum"
        val outputCount = s"output_${idx}_average_count"
        AggregateDescription(
          init = List(
            s"${outputSum}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputSum}->count = 1;",
            s"${outputCount}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputCount}->count = 1;",
            s"double ${cleanName}_accumulated = 0;",
            s"int ${cleanName}_counted = 0;"
          ),
          iter = List(
            s"${cleanName}_accumulated += ${evaluateSub(inputs, sub)};",
          ),
          result = List(
            s"${outputSum}->data[0] = ${cleanName}_accumulated;",
            s"${outputCount}->data[0] = input_0->count;"
          ),
          outputArguments =
            List(s"non_null_double_vector* ${outputSum}", s"non_null_double_vector* ${outputCount}")
        )
      case Count(el) =>
        val outputCount = s"output_${idx}_count"

        AggregateDescription(
          init = List(
            s"${outputCount}->data = (int *)malloc(1 * sizeof(int));",
            s"${outputCount}->count = 1;",
            s"int ${cleanName}_counted = 0;"

          ),
          iter = List(
            s"${cleanName}_counted += 1;"
          ),
          result = List(
            s"${outputCount}->data[0] = ${cleanName}_counted;"
          ),
          outputArguments = List(
            s"non_null_int_vector* ${outputCount}"
          )

        )
            
      case Min(sub) =>
        val outputMin = s"output_${idx}_min"
        AggregateDescription(
          init = List(
            s"${outputMin}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputMin}->count = 1;",
            s"double ${cleanName}_min = std::numeric_limits<double>::max();"
          ),
          iter = List(
            s"if (${cleanName}_min > ${evaluateSub(inputs, sub)}) ${cleanName}_min = ${evaluateSub(inputs, sub)};"
          ),
          result = List(
            s"${outputMin}->data[0] = ${cleanName}_min;",
          ),
          outputArguments = List(
              s"non_null_double_vector* ${outputMin}",
          )
        )

      case Max(sub) =>
        val outputMax = s"output_${idx}_max"
        AggregateDescription(
          init = List(
            s"${outputMax}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputMax}->count = 1;",
            s"double ${cleanName}_max = std::numeric_limits<double>::min();"
          ),
          iter = List(
            s"if (${cleanName}_max < ${evaluateSub(inputs, sub)}) ${cleanName}_max = ${evaluateSub(inputs, sub)};"
          ),
          result = List(
            s"${outputMax}->data[0] = ${cleanName}_max;",
          ),
          outputArguments = List(
            s"non_null_double_vector* ${outputMax}",
          )
        )

      case Corr(left, right, _) =>
        val outputCorr = s"output_${idx}_corr"

        AggregateDescription(
          init = List(
            s"${outputCorr}->data = (double *)malloc(1 * sizeof(double));",
            s"${outputCorr}->count = 1;",

            s"double ${cleanName}_x_sum = 0;",
            s"double ${cleanName}_y_sum = 0;",
            s"double ${cleanName}_xy_sum = 0;",
            s"double ${cleanName}_x_square_sum = 0;",
            s"double ${cleanName}_y_square_sum = 0;"
          ),
          iter = List(
            s"${cleanName}_x_sum += ${evaluateSub(inputs, left)};",
            s"${cleanName}_y_sum += ${evaluateSub(inputs, right)};",
            s"${cleanName}_xy_sum += ${evaluateSub(inputs, left)} * ${evaluateSub(inputs, right)};",
            s"${cleanName}_x_square_sum += ${evaluateSub(inputs, left)} * ${evaluateSub(inputs, left)};",
            s"${cleanName}_y_square_sum += ${evaluateSub(inputs, right)} * ${evaluateSub(inputs, right)};"
          ),
          result = List(
            s"${outputCorr}->data[0] = (input_0->count * ${cleanName}_xy_sum - ${cleanName}_x_sum * ${cleanName}_y_sum) / " + 
              s"sqrt(" + 
                s"(input_0->count * ${cleanName}_x_square_sum - ${cleanName}_x_sum * ${cleanName}_x_sum) * " +
                s"(input_0->count * ${cleanName}_y_square_sum - ${cleanName}_y_sum * ${cleanName}_y_sum));"
          ),
          outputArguments = List(
            s"non_null_double_vector* ${outputCorr}"
          )
        )
        
      }
  }

  final case class CodeLines(lines: List[String]) {
    override def toString: String = (List(s"CodeLines(") ++ lines ++ List(")")).mkString("\n")
  }

  implicit class RichListStr(list: List[String]) {
    def codeLines: CodeLines = CodeLines(list)
  }
  trait NameCleaner {
    def cleanName(input: String): String
  }

  object NameCleaner {
    val simple: NameCleaner = _.replaceAll("[^A-Z_a-z0-9]", "")
    val verbose: NameCleaner = v => CleanName.fromString(v).value
  }

  def cGen(fName: String, input: Seq[Attribute], pairs: (Alias, AggregateExpression)*)(implicit
    nameCleaner: NameCleaner
  ): CodeLines = {
    val cleanNames = pairs.map(_._1.name).map(nameCleaner.cleanName).toList
    val ads = cleanNames.zip(pairs).zipWithIndex.map {
      case ((cleanName, (alias, aggregateExpression)), idx) =>
        process(input, cleanName, aggregateExpression, idx)
          .getOrElse(sys.error(s"Unknown: ${aggregateExpression}"))
    }

    val inputBits = input.zipWithIndex
      .map { case (i, idx) =>
        s"non_null_double_vector* input_${idx}"
      }
      .mkString(", ")

    List[List[String]](
      List(s"""extern "C" long ${fName}(${inputBits}, ${ads
        .flatMap(_.outputArguments)
        .mkString(", ")}) {"""),
      ads.flatMap(_.init),
      List("#pragma _NEC ivdep"),
      List("for (int i = 0; i < input_0->count; i++) {"),
      ads.flatMap(_.iter),
      List("}"),
      ads.flatMap(_.result),
      List("return 0;")
    ).flatten.codeLines
  }
}
