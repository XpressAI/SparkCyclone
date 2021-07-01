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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Multiply

object CExpressionEvaluation {

  final case class AggregateDescription(
    init: List[String],
    iter: List[String],
    result: List[String],
    outputArgument: String
  )

  def evaluateSub(expression: Expression): String = {
    expression match {
      case AttributeReference(_, _, _, _) =>
        // todo support multiple columns - not yet though!
        "input->data[i]"
      case Subtract(left, right, _)             => s"${evaluateSub(left)} - ${evaluateSub(right)}"
      case Multiply(left, right, _)             => s"${evaluateSub(left)} * ${evaluateSub(right)}"
      case Add(left, right, _)                  => s"${evaluateSub(left)} + ${evaluateSub(right)}"
      case Literal(v, DoubleType | IntegerType) => s"$v"
    }
  }

  def process(
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
          iter = List(s"${cleanName}_accumulated += ${evaluateSub(sub)};"),
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
            s"${cleanName}_accumulated += ${evaluateSub(sub)};",
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
  def cGen(pairs: (Alias, AggregateExpression)*): CodeLines = {
    // todo a better clean up - this can clash
    val cleanNames = pairs.map(_._1.name.replaceAll("[^A-Z_a-z0-9]", "")).toList
    val ads = cleanNames.zip(pairs).zipWithIndex.map {
      case ((cleanName, (alias, aggregateExpression)), idx) =>
        process(cleanName, aggregateExpression, idx)
          .getOrElse(sys.error(s"Unknown: ${aggregateExpression}"))
    }

    List[List[String]](
      List(s"""extern "C" long f(non_null_double_vector* input, ${ads
        .map(_.outputArgument)
        .mkString(", ")}) {"""),
      ads.flatMap(_.init),
      List("for (int i = 0; i < input->count; i++) {"),
      ads.flatMap(_.iter),
      List("}"),
      ads.flatMap(_.result),
      List("return 0;")
    ).flatten.codeLines
  }
}
