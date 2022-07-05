/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.sparkcyclone.spark.agile

import io.sparkcyclone.spark.agile.core.CodeLines
import io.sparkcyclone.spark.agile.CFunctionGeneration.CExpression
import io.sparkcyclone.spark.agile.StringHole.StringHoleEvaluation
import io.sparkcyclone.spark.agile.StringHole.StringHoleEvaluation.{
  DateCastStringHoleEvaluation,
  InStringHoleEvaluation,
  LikeStringHoleEvaluation,
  ScalarInExpHoleEvaluation,
}

import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Cast,
  Contains,
  EndsWith,
  EqualTo,
  Expression,
  In,
  IsNotNull,
  LeafExpression,
  Like,
  Literal,
  StartsWith,
  Unevaluable
}
import org.apache.spark.sql.types.{DataType, DateType, StringType}

/**
 * An expression which takes a String and processes it as a vector
 */
final case class StringHole(originalExpression: Expression, evaluation: StringHoleEvaluation)
  extends LeafExpression
  with Unevaluable {
  override def nullable: Boolean = originalExpression.nullable

  override def dataType: DataType = originalExpression.dataType
}

object StringHole {

  sealed trait StringHoleEvaluation {
    def computeVector: CodeLines
    def fetchResult: CExpression
  }

  object StringHoleEvaluation {

    object LikeStringHoleEvaluation {
      final case class Like(refName: String, subject: String) {
        def startsWith: LikeStringHoleEvaluation = LikeStringHoleEvaluation(refName, s"$subject%")
        def endsWith: LikeStringHoleEvaluation = LikeStringHoleEvaluation(refName, s"%$subject")
        def contains: LikeStringHoleEvaluation = LikeStringHoleEvaluation(refName, s"%$subject%")
        def equalsTo: LikeStringHoleEvaluation = LikeStringHoleEvaluation(refName, s"$subject")
      }
    }

    final case class InStringHoleEvaluation(ref: String, valueList: Seq[String])
      extends StringHoleEvaluation {
      val values = s"values_${Math.abs(hashCode())}"
      val elements = s"elements_${Math.abs(hashCode())}"
      val output = s"bitmask_${Math.abs(hashCode())}"
      val delimiter = {
        /*
          Use an ASCII character that is not present in any of the strings in
          the IN query valueList and is not \0.  This avoids incorrect
          tokenization of filter words in frovedis::split_to_words() when one
          of the strings in the IN query valueList contains the delimiter
          character.
         */
        val chars = valueList.mkString("")
        1.to(255).find(x => !chars.contains(x.toChar)) match {
          case Some(d) => d
          case None =>
            throw new IllegalArgumentException(
              "Cannot find a unique ASCII char to use as delimiter"
            )
        }
      }
      val words = valueList.mkString(delimiter.toChar.toString).map(_.toInt).mkString(",")

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"std::vector<size_t> ${output};",
          CodeLines.scoped(s"Evaluate bitmask for ${ref} IN values of the given set") {
            CodeLines.from(
              s"std::vector<int32_t> ${values} { ${words} };",
              s"""frovedis::words ${elements} = frovedis::split_to_words(${values}, std::string(1, char(${delimiter})));""",
              s"${output} = ${ref}->eval_in(${elements});"
            )
          }
        )
      }

      override def fetchResult: CExpression = CExpression(s"${output}[i]", None)
    }

    final case class ScalarInExpHoleEvaluation(ref: String, dtype: DataType, values: Seq[String])
      extends StringHoleEvaluation {
      val output = s"bitmask_${Math.abs(hashCode())}"
      val ctype = SparkExpressionToCExpression.sparkTypeToScalarVeType(dtype).cScalarType

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"// Evaluate bitmask for ${ref} IN values of the given set",
          s"std::vector<size_t> ${output} = ${ref}->eval_in(std::vector<${ctype}> { ${values.mkString(", ")} });",
          ""
        )
      }

      override def fetchResult: CExpression = CExpression(s"${output}[i]", None)
    }

    final case class DateCastStringHoleEvaluation(ref: String) extends StringHoleEvaluation {
      val output = s"dates_${Math.abs(hashCode())}"

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"""// CAST date strings into days (offset from 1970-01-01)""",
          s"std::vector<int32_t> ${output} = ${ref}->date_cast();",
          ""
        )
      }

      override def fetchResult: CExpression = CExpression(s"${output}[i]", None)
    }

    final case class LikeStringHoleEvaluation(ref: String, pattern: String)
      extends StringHoleEvaluation {
      val output = s"bitmask_${Math.abs(hashCode())}"

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"""// Populate ${output} with "${ref} ILIKE '${pattern}'"""",
          s"""std::vector<size_t> ${output} = ${ref}->eval_like("${pattern}");""",
          ""
        )
      }

      override def fetchResult: CExpression = CExpression(s"$output[i]", None)
    }

    final case class ValidityExprHoleEvaluation(ref: String) extends StringHoleEvaluation {
      val output = s"bitmask_${Math.abs(hashCode)}"

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"""// Populate ${output} with the validity buffer of ${ref}""",
          s"std::vector<int32_t> ${output} = ${ref}->validity_vec();",
          ""
        )
      }

      override def fetchResult: CExpression = CExpression(s"$output[i]", None)
    }
  }

  def processPF: PartialFunction[Expression, StringHoleTransformation] =
    Function.unlift(process)

  def process(orig: Expression): Option[StringHoleTransformation] = {
    val exprWithHoles = orig.transform {
      case exp if processSubExpression.isDefinedAt(exp) =>
        StringHole(exp, processSubExpression.apply(exp))
    }

    Option {
      exprWithHoles.collect { case sh @ StringHole(_, _) =>
        sh
      }
    }.filter(_.nonEmpty)
      .map(map =>
        StringHoleTransformation(
          exprWithHoles = exprWithHoles,
          holes = map.map(sh => sh -> sh.evaluation).toMap
        )
      )
  }

  def processSubExpression: PartialFunction[Expression, StringHoleEvaluation] = {
    case Like(left: AttributeReference, Literal(v, StringType), _) =>
      LikeStringHoleEvaluation(left.name, v.toString)
    case StartsWith(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).startsWith
    case EndsWith(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).endsWith
    case Contains(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).contains
    case EqualTo(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).equalsTo
    case IsNotNull(item: AttributeReference) if item.dataType == StringType =>
      StringHoleEvaluation.ValidityExprHoleEvaluation(item.name)
    case Cast(expr: AttributeReference, DateType, Some(_)) =>
      DateCastStringHoleEvaluation(expr.name)
    case In(expr: AttributeReference, values: Seq[Literal]) if expr.dataType == StringType =>
      InStringHoleEvaluation(expr.name, values.map(_.toString))
    case In(expr: AttributeReference, values: Seq[Literal]) =>
      /*
        NOTE: The ad-hoc removal of `->data[i]` is a workaround.  The real fix is
        to update `SparkExpressionToCExpression.referenceReplacer`, but since the
        rest of the `VERewriteStrategy` and `SparkExpressionToCExpression.eval`
        uses the output of that function, it is preferable to make the necessary
        changes in a future dedicated PR.
       */
      ScalarInExpHoleEvaluation(
        expr.name.replace("->data[i]", ""),
        expr.dataType,
        values.map(_.toString)
      )
  }

  def transform: PartialFunction[Expression, Expression] = Function
    .unlift(process)
    .andThen(_.newExpression)

  final case class StringHoleTransformation(
    exprWithHoles: Expression,
    holes: Map[StringHole, StringHoleEvaluation]
  ) {
    def stringParts: List[StringHoleEvaluation] = holes.values.toList

    def newExpression: Expression = exprWithHoles
  }
}
