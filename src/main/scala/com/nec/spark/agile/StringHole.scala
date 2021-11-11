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
import com.nec.spark.agile.StringHole.StringHoleEvaluation.SlowEvaluator.SlowEvaluator
import com.nec.spark.agile.StringHole.StringHoleEvaluation.{
  LikeStringHoleEvaluation,
  SlowEvaluation,
  SlowEvaluator
}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Contains,
  EndsWith,
  EqualTo,
  Expression,
  LeafExpression,
  Literal,
  StartsWith,
  Unevaluable
}
import org.apache.spark.sql.types.{DataType, StringType}

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

    /** Vectorized evaluation */
    final case class LikeStringHoleEvaluation(refName: String, likeString: String)
      extends StringHoleEvaluation {
      val myId = s"slowStringEvaluation_${Math.abs(hashCode())}"
      val myIdWords = s"slowStringEvaluation_words_${Math.abs(hashCode())}"
      val matchingIds = s"slowStringEvaluation_matching_ids_${Math.abs(hashCode())}"
      override def computeVector: CodeLines =
        CodeLines.from(
          CodeLines.debugHere,
          s"std::vector<int> $myId($refName->count);",
          s"frovedis::words $myIdWords = varchar_vector_to_words($refName);",
          s"std::vector<size_t> $matchingIds = frovedis::like($myIdWords.chars," +
            s"(const vector<size_t>&)($myIdWords.starts),",
          s"(const vector<size_t>&)($myIdWords.lens),",
          s""""$likeString");""",
          CodeLines.debugHere,
          s"for ( int i = 0; i < $refName->count; i++) { ",
          CodeLines
            .from(s"$myId[i] = 0;")
            .indented,
          "}",
          s"for(int i = 0; i < $matchingIds.size(); i++) {",
          CodeLines.from(s"$myId[$matchingIds[i]] = 1;"),
          "}"
        )

      override def deallocData: CodeLines = CodeLines.empty

      /** Fetch result per each item - most likely an int */
      override def fetchResult: CExpression = CExpression(s"$myId[i]", None)
    }

    final case class SlowEvaluation(refName: String, slowEvaluator: SlowEvaluator)
      extends StringHoleEvaluation {
      val myId = s"slowStringEvaluation_${Math.abs(hashCode())}"
      override def computeVector: CodeLines =
        CodeLines.from(
          CodeLines.debugHere,
          s"std::vector<int> $myId($refName->count);",
          s"for ( int i = 0; i < $refName->count; i++) { ",
          CodeLines
            .from(s"$myId[i] = ${slowEvaluator.evaluate(refName).cCode};")
            .indented,
          "}"
        )

      override def deallocData: CodeLines = CodeLines.empty

      override def fetchResult: CExpression = CExpression(s"$myId[i]", None)
    }
    object SlowEvaluator {
      sealed trait SlowEvaluator {
        def evaluate(refName: String): CExpression
      }
      final case class StartsWithEvaluator(theString: String) extends SlowEvaluator {
        override def evaluate(refName: String): CExpression = {
          val leftStringLength =
            s"($refName->offsets[i+1] - $refName->offsets[i])"
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
    case StartsWith(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).startsWith
    case EndsWith(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).endsWith
    case Contains(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).contains
    case EqualTo(left: AttributeReference, Literal(v, StringType)) =>
      LikeStringHoleEvaluation.Like(left.name, v.toString).equalsTo
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
        s"std::string($leftRef->data, $leftRef->offsets[i], $leftRef->offsets[i+1]-$leftRef->offsets[i])",
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
          s"($leftRef->offsets[i+1] - $leftRef->offsets[i])"
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
          s"($leftRef->offsets[i+1] - $leftRef->offsets[i])"
        val expectedLength = right.length
        val leftStringSubstring =
          s"""std::string($leftRef->data, $leftRef->offsets[i+1]-$expectedLength, $expectedLength)"""
        val rightString = s"""std::string("$right")"""
        s"$leftStringLength >= $expectedLength && $leftStringSubstring == $rightString"
      },
      isNotNullCode = None
    )

  private def containsExp(leftRef: String, right: String): CExpression =
    CExpression(
      cCode = {
        val mainString =
          s"std::string($leftRef->data, $leftRef->offsets[i], $leftRef->offsets[i+1]-$leftRef->offsets[i])"
        val rightString = s"""std::string("$right")"""
        s"$mainString.find($rightString) != std::string::npos"
      },
      isNotNullCode = None
    )
}