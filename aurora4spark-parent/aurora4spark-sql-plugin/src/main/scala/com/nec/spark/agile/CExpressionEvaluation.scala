package com.nec.spark.agile
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.NamedExpression

object CExpressionEvaluation {
  def cGenProject(inputs: Seq[Attribute], resultExpressions: Seq[NamedExpression])(implicit
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
      List(s"""extern "C" long f(${arguments.mkString(", ")})""", "{"),
      resultExpressions.zipWithIndex.flatMap { case (res, idx) =>
        List(
          s"output_${idx}->count = input_0->count;",
          s"output_${idx}->data = (double*) malloc(output_${idx}->count * sizeof(double));"
        )
      }.toList,
      List("#pragma omp parallel for", "for (int i = 0; i < output_0->count; i++) {"),
      resultExpressions.zipWithIndex.flatMap {
        case (re: Alias, idx) =>
          List(s"output_${idx}->data[i] = ${evaluateSub(inputs, re.child)};")
        case (other, idx) =>
          sys.error(s"Got ${other}, expected an Alias, at index ${idx}.")
      }.toList,
      List("}", "return 0;", "}")
    ).flatten.codeLines
  }

  final case class AggregateDescription(
    init: List[String],
    iter: List[String],
    result: List[String],
    outputArgument: String
  )

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
            s"output_${idx}->data = (double *)malloc(1 * sizeof(double));",
            s"double ${cleanName}_accumulated = 0;"
          ),
          iter = List(s"${cleanName}_accumulated += ${evaluateSub(inputs, sub)};"),
          result = List(
            s"double ${cleanName}_result = ${cleanName}_accumulated;",
            s"output_${idx}->data[0] = ${cleanName}_result;"
          ),
          outputArgument = s"non_null_double_vector* output_${idx}"
        )
      case Average(sub) =>
        AggregateDescription(
          init = List(
            s"output_${idx}->data = (double *)malloc(1 * sizeof(double));",
            s"double ${cleanName}_accumulated = 0;",
            s"int ${cleanName}_counted = 0;"
          ),
          iter = List(
            s"${cleanName}_accumulated += ${evaluateSub(inputs, sub)};",
            s"${cleanName}_counted += 1;"
          ),
          result = List(
            s"double ${cleanName}_result = ${cleanName}_accumulated / ${cleanName}_counted;",
            s"output_${idx}->data[0] = ${cleanName}_result;"
          ),
          outputArgument = s"non_null_double_vector* output_${idx}"
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

  def cGen(input: Seq[Attribute], pairs: (Alias, AggregateExpression)*)(implicit
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
      List(s"""extern "C" long f(${inputBits}, ${ads
        .map(_.outputArgument)
        .mkString(", ")}) {"""),
      ads.flatMap(_.init),
      List("for (int i = 0; i < input_0->count; i++) {"),
      ads.flatMap(_.iter),
      List("}"),
      ads.flatMap(_.result),
      List("return 0;")
    ).flatten.codeLines
  }
}
