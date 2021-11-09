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
import com.nec.spark.agile.StringHole.StringHoleEvaluation.HoleStartsWith
import org.apache.spark.sql.catalyst.expressions.{
  CaseWhen,
  Expression,
  LeafExpression,
  StartsWith,
  Unevaluable
}
import org.apache.spark.sql.types.DataType

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
    final case class HoleStartsWith() extends StringHoleEvaluation {
      override def computeVector: CodeLines = CodeLines.empty
      override def deallocData: CodeLines = CodeLines.empty
      override def fetchResult: CExpression = CExpression(s"abcd", None)
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
    case e @ StartsWith(l, r) => HoleStartsWith()
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
