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
import com.nec.spark.agile.StringHole.StringHoleEvaluation.{SlowEvaluation, SlowEvaluator}
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
    sealed trait SlowEvaluator {
      def evaluate(refName: String): CExpression
    }

    object SlowEvaluator {
      final case class StartsWithEvaluator(theString: String) extends SlowEvaluator {
        override def evaluate(refName: String): CExpression = {

          val leftStringLength =
            s"(${refName}->offsets[i+1] - ${refName}->offsets[i])"
          val expectedLength = theString.length
          val leftStringSubstring =
            s"""std::string(${refName}->data, ${refName}->offsets[i], ${expectedLength})"""
          val rightString = s"""std::string("${theString}")"""
          val bool =
            s"${leftStringLength} >= ${expectedLength} && ${leftStringSubstring} == ${rightString}"
          CExpression(bool, None)
        }
      }
    }

    final case class SlowEvaluation(refName: String, slowEvaluator: SlowEvaluator)
      extends StringHoleEvaluation {
      override def computeVector: CodeLines =
        CodeLines.from(
          s"std::vector<int> startsWith_${refName}(${refName}->count);",
          s"for ( int i = 0; i < ${refName}->count; i++) { ",
          CodeLines
            .from(s"startsWith_${refName}[i] = ${slowEvaluator.evaluate(refName).cCode};")
            .indented,
          "}"
        )

      override def deallocData: CodeLines = CodeLines.empty

      override def fetchResult: CExpression = CExpression(s"startsWith_${refName}[i]", None)
    }
  }
  def process(orig: Expression): Option[StringHoleTransformation] = Option {
    StringHoleTransformation(
      exprWithHoles = orig.transform {
        case exp if processSubExpression.isDefinedAt(exp) =>
          StringHole(exp, processSubExpression.apply(exp))
      },
      holes = orig.collect {
        case exp if processSubExpression.isDefinedAt(exp) =>
          StringHole(exp, processSubExpression.apply(exp)) -> processSubExpression.apply(exp)
      }.toMap
    )
  }.filter(_.exprWithHoles != orig)

  def processSubExpression: PartialFunction[Expression, StringHoleEvaluation] = {
    case StartsWith(left: AttributeReference, Literal(v, StringType)) =>
      SlowEvaluation(left.name, SlowEvaluator.StartsWithEvaluator(v.toString))
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

  def equalTo(leftRef: String, rightStr: String): CExpression = {
    CExpression(
      cCode = List(
        s"std::string(${leftRef}->data, ${leftRef}->offsets[i], ${leftRef}->offsets[i+1]-${leftRef}->offsets[i])",
        s"""std::string("${rightStr}")"""
      ).mkString(" == "),
      isNotNullCode = None
    )

  }

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

  private def startsWithExp(leftRef: String, right: String) = {
    CExpression(
      cCode = {
        val leftStringLength =
          s"(${leftRef}->offsets[i+1] - ${leftRef}->offsets[i])"
        val expectedLength = right.length
        val leftStringSubstring =
          s"""std::string(${leftRef}->data, ${leftRef}->offsets[i], ${expectedLength})"""
        val rightString = s"""std::string("${right}")"""
        s"${leftStringLength} >= ${expectedLength} && ${leftStringSubstring} == ${rightString}"
      },
      isNotNullCode = None
    )
  }

  private def endsWithExp(leftRef: String, right: String) = {
    CExpression(
      cCode = {
        val leftStringLength =
          s"(${leftRef}->offsets[i+1] - ${leftRef}->offsets[i])"
        val expectedLength = right.length
        val leftStringSubstring =
          s"""std::string(${leftRef}->data, ${leftRef}->offsets[i+1]-${expectedLength}, ${expectedLength})"""
        val rightString = s"""std::string("${right}")"""
        s"${leftStringLength} >= ${expectedLength} && ${leftStringSubstring} == ${rightString}"
      },
      isNotNullCode = None
    )
  }

  private def containsExp(leftRef: String, right: String) = {
    CExpression(
      cCode = {
        val mainString =
          s"std::string(${leftRef}->data, ${leftRef}->offsets[i], ${leftRef}->offsets[i+1]-${leftRef}->offsets[i])"
        val rightString = s"""std::string("${right}")"""
        s"${mainString}.find(${rightString}) != std::string::npos"
      },
      isNotNullCode = None
    )
  }
}
