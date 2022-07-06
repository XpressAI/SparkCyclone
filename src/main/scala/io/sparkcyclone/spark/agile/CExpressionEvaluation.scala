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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.types._
import scala.annotation.tailrec
import scala.language.implicitConversions

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

  trait NameCleaner {
    def cleanName(input: String): String
  }

  object NameCleaner {
    val simple: NameCleaner = _.replaceAll("[^A-Z_a-z0-9]", "")
    val verbose: NameCleaner = v => CleanName.fromString(v).value
  }
}
