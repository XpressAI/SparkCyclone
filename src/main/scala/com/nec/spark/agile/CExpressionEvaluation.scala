package com.nec.spark.agile

import com.nec.cmake.UdpDebug
import com.nec.spark.planning.NativeAggregationEvaluationPlan.TracerDefName

import scala.language.implicitConversions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.types._

import scala.annotation.tailrec

object CExpressionEvaluation {

  final case class AggregateDescription(
    init: List[String],
    iter: List[String],
    result: List[String],
    outputArguments: List[String]
  )

  def evaluateExpression(input: Seq[Attribute], expression: Expression): String = {
    expression match {
      case NormalizeNaNAndZero(child)          => evaluateExpression(input, child)
      case KnownFloatingPointNormalized(child) => evaluateExpression(input, child)
      case alias @ Alias(expr, name)           => evaluateSub(input, alias.child)
      case expr @ NamedExpression(
            name,
            DoubleType | FloatType | LongType | IntegerType | ShortType
          ) =>
        input.indexWhere(_.exprId == expr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx => s"input_${idx}->data[i]"
        }
      case AttributeReference(name, typeName, _, _) =>
        (input.indexWhere(_.name == name), typeName) match {
          case (-1, typeName) =>
            sys.error(
              s"Could not find a reference for '${expression}' with type: ${typeName} from set of: ${input}"
            )
          case (idx, DoubleType | FloatType | LongType | IntegerType | ShortType) =>
            s"input_${idx}->data[i]"
          case (idx, actualType) => sys.error(s"'${expression}' has unsupported type: ${typeName}")
        }
      case expr @ NamedExpression(
            name,
            DoubleType | FloatType | LongType | IntegerType | ShortType
          ) =>
        input.indexWhere(_.exprId == expr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for '${expression}' from set of: ${input}")
          case idx => s"input_${idx}->data[i]"
        }
      case Cast(child, dataType, _) =>
        val expr = evaluateExpression(input, child)
        dataType match {
          case ShortType   => s"((int16_t)$expr)"
          case IntegerType => s"((int32_t)$expr)"
          case LongType    => s"((int64_t)$expr)"
          case FloatType   => s"((float)$expr)"
          case DoubleType  => s"((double)$expr)"
        }
    }
  }

  def evaluateSub(inputs: Seq[Attribute], expression: Expression): String = {
    expression match {
      case attr @ AttributeReference(name, _, _, _) =>
        inputs.indexWhere(_.exprId == attr.exprId) match {
          case -1 =>
            sys.error(s"Could not find a reference for ${expression} from set of: ${inputs}")
          case idx =>
            s"input_${idx}->data[i]"
        }
      case Subtract(left, right, _) =>
        s"${evaluateSub(inputs, left)} - ${evaluateSub(inputs, right)}"
      case DateSub(startDate, days) =>
        s"${evaluateSub(inputs, startDate)} - ${evaluateSub(inputs, days)}"
      case DateAdd(startDate, days) =>
        s"${evaluateSub(inputs, startDate)} + ${evaluateSub(inputs, days)}"
      case Multiply(left, right, _) =>
        s"${evaluateSub(inputs, left)} * ${evaluateSub(inputs, right)}"
      case Add(left, right, _) =>
        s"${evaluateSub(inputs, left)} + ${evaluateSub(inputs, right)}"
      case Divide(left, right, _) =>
        s"${evaluateSub(inputs, left)} / ${evaluateSub(inputs, right)}"
      case Abs(v) =>
        s"abs(${evaluateSub(inputs, v)})"
      case Literal(v, DoubleType | FloatType | LongType | IntegerType | DateType | ShortType) =>
        s"$v"
      case And(left, right) =>
        s"${evaluateSub(inputs, left)} && ${evaluateSub(inputs, right)}"
      case IsNotNull(_) =>
        s"1"
      case LessThan(left, right) =>
        s"${evaluateSub(inputs, left)} < ${evaluateSub(inputs, right)}"
      case GreaterThan(left, right) =>
        s"${evaluateSub(inputs, left)} > ${evaluateSub(inputs, right)}"
      case LessThanOrEqual(left, right) =>
        s"${evaluateSub(inputs, left)} < ${evaluateSub(inputs, right)}"
      case Cast(child, dataType, _) =>
        dataType match {
          case LongType    => s"((int64_t)${evaluateSub(inputs, child)})"
          case IntegerType => s"((int32_t)${evaluateSub(inputs, child)})";
          case FloatType   => s"((float)${evaluateSub(inputs, child)})";
          case DoubleType  => s"((double)${evaluateSub(inputs, child)})";
        }
    }
  }

  def shortenLines(lines: List[String]): List[String] = {

    @tailrec
    def rec(charsSoFar: Int, remaining: List[String], linesSoFar: List[String]): List[String] = {
      remaining match {
        case Nil => linesSoFar
        case one :: rest if one.length + charsSoFar > 100 =>
          linesSoFar ++ List("...")
        case one :: rest =>
          rec(charsSoFar + one.length, rest, linesSoFar ++ List(one))
      }
    }

    rec(0, lines, Nil)

  }

  final case class CodeLines(lines: List[String]) {

    def ++(other: CodeLines): CodeLines = CodeLines(lines = lines ++ (" " :: other.lines))

    def block: CodeLines = CodeLines.from("", "{", this.indented, "}", "")

    def blockCommented(str: String): CodeLines =
      CodeLines.from(s"// ${str}", "{", this.indented, "}", "")

    def indented: CodeLines = CodeLines(lines = lines.map(line => s"  $line"))

    override def toString: String =
      (List(s"CodeLines(") ++ shortenLines(lines) ++ List(")")).mkString("\n")

    def cCode: String = lines.mkString("\n", "\n", "\n")

    def append(codeLines: CodeLines*): CodeLines = copy(lines = lines ++ codeLines.flatMap(_.lines))
  }

  object CodeLines {

    def debugHere(implicit fullName: sourcecode.FullName, line: sourcecode.Line): CodeLines =
      CodeLines.from(
        UdpDebug
          .Conditional(TracerDefName, UdpDebug.conditional)
          .send(
            "utcnanotime().c_str()",
            """";"""",
            s"std::string(${TracerDefName}->data, ${TracerDefName}->offsets[0], ${TracerDefName}->offsets[1] - ${TracerDefName}->offsets[0])",
            """";"""",
            s"${TracerDefName}->size",
            ";",
            s"${TracerDefName}->count",
            ";",
            s"${TracerDefName}->offsets[0]",
            ";",
            s"${TracerDefName}->offsets[1]",
            ";",
            s""""${fullName.value}#${line.value}"""",
            """" """"
          ),
        "#ifdef DEBUG",
        s"""std::cout << utcnanotime().c_str() << " $$ " << "${fullName.value} (#${line.value}/#" << __LINE__ << ")" << std::endl << std::flush;""",
        "#endif"
      )

    def commentHere(
      what: String*
    )(implicit fullName: sourcecode.FullName, line: sourcecode.Line): CodeLines =
      CodeLines.from(
        what.map(w => CodeLines.from(s"// $w")).toList,
        s"// ${fullName.value} (#${line.value})"
      )

    def from(str: CodeLines*): CodeLines = CodeLines(lines = str.flatMap(_.lines).toList)

    implicit def stringToCodeLines(str: String): CodeLines = CodeLines(List(str))

    implicit def listStringToCodeLines(str: List[String]): CodeLines = CodeLines(str)

    implicit def listCodeLines(str: List[CodeLines]): CodeLines = CodeLines(str.flatMap(_.lines))

    def empty: CodeLines = CodeLines(Nil)
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
}
