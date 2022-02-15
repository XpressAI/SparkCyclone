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
package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CExpression
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.StringHole.StringHoleEvaluation.SlowEvaluator.{
  NotNullEvaluator,
  SlowEvaluator
}
import com.nec.spark.agile.StringHole.StringHoleEvaluation.{
  DateCastStringHoleEvaluation,
  InStringHoleEvaluation,
  LikeStringHoleEvaluation,
  ScalarInExpHoleEvaluation,
  SlowEvaluation,
  SlowEvaluator
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

    def deallocData: CodeLines

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

    final case class InStringHoleEvaluation(refName: String, valueList: Seq[String])
      extends StringHoleEvaluation {
      val inputWords = s"input_words_${Math.abs(hashCode())}"
      val values = s"values_${Math.abs(hashCode())}"
      val filteringSet = s"filtering_set_${Math.abs(hashCode())}"
      val matchingIds = s"matching_ids_${Math.abs(hashCode())}"
      val filteredIds = s"filtered_ids_${Math.abs(hashCode())}"
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
          s"""std::cout << "COUNT: " << ${refName}->count << std::endl; """,
          s"std::vector<size_t> ${filteredIds}(${refName}->count);",
          CodeLines.scoped(s"Filter ${refName} to values of the given set") {
            CodeLines.from(
              s"frovedis::words $inputWords = $refName->to_words();",
              s"std::vector<int> ${values} { ${words} };",
              s"""frovedis::words ${filteringSet} = frovedis::split_to_words(${values}, std::string(1, char(${delimiter})));""",
              s"std::vector<size_t> ${matchingIds} = filter_words_dict(${inputWords}, ${filteringSet});",
              CodeLines.forLoop("i", s"${matchingIds}.size()") {
                CodeLines.ifElseStatement(
                  s"$matchingIds[i] != std::numeric_limits<size_t>::max()"
                ) {
                  s"${filteredIds}[i] = 1;"
                } {
                  s"${filteredIds}[i] = 0;"
                }
              }
            )
          }
        )
      }

      override def deallocData: CodeLines = CodeLines.empty

      override def fetchResult: CExpression = CExpression(s"${filteredIds}[i]", None)
    }

    final case class ScalarInExpHoleEvaluation(ref: String, dtype: DataType, values: Seq[String])
      extends StringHoleEvaluation {
      val filteredIds = s"filtered_ids_${Math.abs(hashCode())}"
      val elements = s"elements_${Math.abs(hashCode())}"
      val ctype = SparkExpressionToCExpression.sparkTypeToScalarVeType(dtype)

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"std::vector<size_t> ${filteredIds}(${ref}->count);",
          CodeLines.scoped(s"Filter ${ref} to values of the given set") {
            CodeLines.from(
              // Create the IN elements vector
              s"std::vector<${ctype.cScalarType}> ${elements} { ${values.mkString(", ")} };",
              "",
              // Add pragma to guide NCC to vectorization
              "#pragma _NEC ivdep",
              // Loop over the IN elements first (makes the code more vectorizable)
              CodeLines.forLoop("j", s"${elements}.size()") {
                // Loop over column values
                CodeLines.forLoop("i", s"${filteredIds}.size()") {
                  // Apply ||= to `(element value == column value)`
                  s"${filteredIds}[i] = (${filteredIds}[i] || (${ref}->data[i] == ${elements}[j]));"
                }
              }
            )
          }
        )
      }

      override def deallocData: CodeLines = CodeLines.empty

      override def fetchResult: CExpression = CExpression(s"${filteredIds}[i]", None)
    }

    final case class DateCastStringHoleEvaluation(refName: String) extends StringHoleEvaluation {
      val finalVectorName = s"stringCasting_${Math.abs(hashCode())}"
      val myIdWords = s"stringCasting_words_${Math.abs(hashCode())}"
      val dateTimeVectorName = s"stringCasting_datetime_${Math.abs(hashCode())}"
      override def computeVector: CodeLines = {
        CodeLines.from(
          s"frovedis::words $myIdWords = $refName->to_words();",
          s"""std::vector<datetime_t> $dateTimeVectorName = frovedis::parsedatetime($myIdWords, std::string("%Y-%m-%d"));""",
          s"std::vector<int> $finalVectorName($refName->count);",
          "datetime_t epoch = frovedis::makedatetime(1970, 1, 1, 0, 0, 0, 0);",
          s"for(int i = 0; i < $refName->count; i++) {",
          CodeLines
            .from(
              s"$finalVectorName[i] = frovedis::datetime_diff_day($dateTimeVectorName[i], epoch);"
            )
            .indented,
          "}"
        )
      }

      override def deallocData: CodeLines = CodeLines.empty

      override def fetchResult: CExpression = CExpression(s"$finalVectorName[i]", None)
    }

    /** Vectorized evaluation */
    final case class LikeStringHoleEvaluation(refName: String, likeString: String)
      extends StringHoleEvaluation {
      val myId = s"output_${Math.abs(hashCode())}"
      val myIdWords = s"input_words_${Math.abs(hashCode())}"
      val matchingIds = s"matching_ids_${Math.abs(hashCode())}"

      override def computeVector: CodeLines = {
        CodeLines.from(
          s"std::vector<int> ${myId}($refName->count);",
          CodeLines.scoped(s"Populate ${myId} with ${refName} ILIKE '${likeString}'") {
            CodeLines.from(
              s"frovedis::words $myIdWords = ${refName}->to_words();",
              s"""std::vector<size_t> ${matchingIds} = frovedis::like(${myIdWords}, "${likeString}");""",
              CodeLines.forLoop("i", s"${refName}->count") {
                s"${myId}[i] = 0;"
              },
              CodeLines.forLoop("i", s"${matchingIds}.size()") {
                s"${myId}[${matchingIds}[i]] = 1;"
              }
            )
          }
        )
      }

      override def deallocData: CodeLines = CodeLines.empty

      /** Fetch result per each item - most likely an int */
      override def fetchResult: CExpression = CExpression(s"$myId[i]", None)
    }

    final case class SlowEvaluation(refName: String, slowEvaluator: SlowEvaluator)
      extends StringHoleEvaluation {
      val myId = s"${refName}_sloweval_${Math.abs(hashCode())}"
      override def computeVector: CodeLines = {
        CodeLines.from(
          s"std::vector<int> ${myId}(${refName}->count);",
          CodeLines.scoped(s"Populate ${myId} with slow evaluation of ${refName}") {
            CodeLines.forLoop("i", s"${refName}->count") {
              s"$myId[i] = ${slowEvaluator.evaluate(refName).cCode};"
            }
          }
        )
      }

      override def deallocData: CodeLines = CodeLines.empty

      override def fetchResult: CExpression = CExpression(s"$myId[i]", None)
    }
    object SlowEvaluator {
      sealed trait SlowEvaluator {
        def evaluate(refName: String): CExpression
      }
      case object NotNullEvaluator extends SlowEvaluator {
        override def evaluate(refName: String): CExpression =
          CExpression(cCode = s"${refName}->get_validity(i)", isNotNullCode = None)
      }
      final case class StartsWithEvaluator(theString: String) extends SlowEvaluator {
        override def evaluate(refName: String): CExpression = {
          val leftStringLength =
            s"($refName->offsets[i] + $refName->lengths[i])"
          val expectedLength = theString.length
          val leftStringSubstring =
            s"""std::string($refName->data, $refName->offsets[i], $expectedLength)"""
          val rightString = s"""std::string("$theString")"""
          val bool =
            s"$leftStringLength >= $expectedLength && $leftStringSubstring == $rightString"
          CExpression(bool, None)
        }
      }
      final case class EndsWithEvaluator(theString: String) extends SlowEvaluator {
        override def evaluate(refName: String): CExpression =
          endsWithExp(refName, theString)
      }
      final case class ContainsEvaluator(theString: String) extends SlowEvaluator {
        override def evaluate(refName: String): CExpression =
          containsExp(refName, theString)
      }
      final case class EqualsEvaluator(theString: String) extends SlowEvaluator {
        override def evaluate(refName: String): CExpression =
          equalTo(refName, theString)
      }
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

  def processSubExpressionSlow: PartialFunction[Expression, StringHoleEvaluation] = {
    case StartsWith(left: AttributeReference, Literal(v, StringType)) =>
      SlowEvaluation(left.name, SlowEvaluator.StartsWithEvaluator(v.toString))
    case EndsWith(left: AttributeReference, Literal(v, StringType)) =>
      SlowEvaluation(left.name, SlowEvaluator.EndsWithEvaluator(v.toString))
    case Contains(left: AttributeReference, Literal(v, StringType)) =>
      SlowEvaluation(left.name, SlowEvaluator.ContainsEvaluator(v.toString))
    case EqualTo(left: AttributeReference, Literal(v, StringType)) =>
      SlowEvaluation(left.name, SlowEvaluator.EqualsEvaluator(v.toString))
  }

  private val UseFastMethod: Boolean = true

  def processSubExpression: PartialFunction[Expression, StringHoleEvaluation] =
    if (UseFastMethod) processSubExpressionFast else processSubExpressionSlow

  def processSubExpressionFast: PartialFunction[Expression, StringHoleEvaluation] = {
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
      SlowEvaluation(item.name, NotNullEvaluator)
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

  def equalTo(leftRef: String, rightStr: String): CExpression =
    CExpression(
      cCode = List(
        s"std::string($leftRef->data, $leftRef->offsets[i], $leftRef->offsets[i]+$leftRef->lengths[i])",
        s"""std::string("$rightStr")"""
      ).mkString(" == "),
      isNotNullCode = None
    )

  def simpleStringExpressionMatcher(expression: Expression): Option[CExpression] =
    PartialFunction.condOpt(expression) {
      case EqualTo(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        equalTo(left.name, right.toString())
      case Contains(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        containsExp(left.name, right.toString())
      case EndsWith(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        endsWithExp(left.name, right.toString())
      case StartsWith(left: AttributeReference, right: Literal)
          if left.dataType == StringType && right.dataType == StringType =>
        startsWithExp(left.name, right.toString())
    }

  private def startsWithExp(leftRef: String, right: String): CExpression =
    CExpression(
      cCode = {
        val leftStringLength =
          s"($leftRef->offsets[i] + $leftRef->lengths[i])"
        val expectedLength = right.length
        val leftStringSubstring =
          s"""std::string($leftRef->data, $leftRef->offsets[i], $expectedLength)"""
        val rightString = s"""std::string("$right")"""
        s"$leftStringLength >= $expectedLength && $leftStringSubstring == $rightString"
      },
      isNotNullCode = None
    )

  private def endsWithExp(leftRef: String, right: String): CExpression =
    CExpression(
      cCode = {
        val leftStringLength =
          s"($leftRef->offsets[i] + $leftRef->lengths[i])"
        val expectedLength = right.length
        val leftStringSubstring =
          s"""std::string($leftRef->data, $leftRef->offsets[i]-$expectedLength, $expectedLength)"""
        val rightString = s"""std::string("$right")"""
        s"$leftStringLength >= $expectedLength && $leftStringSubstring == $rightString"
      },
      isNotNullCode = None
    )

  private def containsExp(leftRef: String, right: String): CExpression =
    CExpression(
      cCode = {
        val mainString =
          s"std::string($leftRef->data, $leftRef->offsets[i], $leftRef->offsets[i]+$leftRef->lengths[i])"
        val rightString = s"""std::string("$right")"""
        s"$mainString.find($rightString) != std::string::npos"
      },
      isNotNullCode = None
    )
}
