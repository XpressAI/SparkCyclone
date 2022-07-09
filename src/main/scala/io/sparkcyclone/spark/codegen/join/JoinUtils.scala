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
 package io.sparkcyclone.spark.codegen.join

 import io.sparkcyclone.native.code._
 import io.sparkcyclone.native.code.VeScalarType._
 import io.sparkcyclone.spark.codegen.CFunctionGeneration._
 import io.sparkcyclone.spark.codegen.StringHole.StringHoleEvaluation
 import io.sparkcyclone.spark.codegen.StringProducer.FrovedisStringProducer
 import org.apache.arrow.memory.BufferAllocator
 import org.apache.arrow.vector._

 import org.apache.spark.sql.types._

 object JoinUtils {
  sealed trait JoinType
  case object LeftOuterJoin extends JoinType
  case object RightOuterJoin extends JoinType

  final case class VeInnerJoin[Input, LeftKey, RightKey, Output](
    inputs: List[Input],
    leftKey: LeftKey,
    rightKey: RightKey,
    outputs: List[Output]
  )

  final case class OuterJoinOutput[Output](innerJoinOutputs: Output, outerJoinOutputs: Output)

  final case class VeOuterJoin[Input, LeftKey, RightKey, Output](
    inputs: List[Input],
    leftKey: LeftKey,
    rightKey: RightKey,
    outputs: List[OuterJoinOutput[Output]],
    joinType: JoinType
  )

  sealed trait JoinExpression {
    def fold[T](whenProj: CExpression => T): T
  }

  final case class TypedJoinExpression[ScalaType](joinExpression: JoinExpression)

  object JoinExpression {
    final case class JoinProjection(cExpression: CExpression) extends JoinExpression {
      override def fold[T](whenProj: CExpression => T): T = whenProj(cExpression)
    }

    // TODO: We can use that to meld join and aggregate
    // final case class JoinAggregation(aggregation: Aggregation) extends JoinExpression {
    //   override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenAgg(
    //     aggregation
    //   )
    // }
  }

  final case class NamedJoinExpression(
    name: String,
    veType: VeScalarType,
    joinExpression: JoinExpression
  )

  def renderInnerJoin(
    veInnerJoin: VeInnerJoin[CVector, TypedCExpression2, TypedCExpression2, NamedJoinExpression]
  ): CFunction = {
    CFunction(
      inputs = veInnerJoin.inputs,
      outputs = veInnerJoin.outputs.zipWithIndex.map {
        case (NamedJoinExpression(outputName, veType, _), idx) =>
          CScalarVector(outputName, veType)
      },
      body = CodeLines.from(
        s"std::vector <${veInnerJoin.leftKey.veType.cScalarType}> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <${veInnerJoin.rightKey.veType.cScalarType}> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        s"for(int i = 0; i < ${veInnerJoin.leftKey.cExpression.cCode
          .replace("data[i]", "count")}; i++) { ",
        CodeLines
          .from(
            s"left_vec.push_back(${veInnerJoin.leftKey.cExpression.cCode});",
            "left_idx.push_back(i);"
          )
          .indented,
        "}",
        s"for(int i = 0; i < ${veInnerJoin.rightKey.cExpression.cCode
          .replace("data[i]", "count")}; i++) { ",
        CodeLines
          .from(
            s"right_vec.push_back(${veInnerJoin.rightKey.cExpression.cCode});",
            "right_idx.push_back(i);"
          )
          .indented,
        "}",
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        s"frovedis::equi_join<${veInnerJoin.leftKey.veType.cScalarType}>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);",
        "long validityBuffSize = frovedis::ceil_div(size_t(left_out.size()), size_t(64));",
        veInnerJoin.outputs.map { case NamedJoinExpression(outputName, veType, joinExpression) =>
          joinExpression.fold(whenProj =
            _ =>
              CodeLines.from(
                s"${outputName}->data = static_cast<${veType.cScalarType}*>(malloc(left_out.size() * sizeof(${veType.cScalarType})));",
                s"${outputName}->validityBuffer = static_cast<uint64_t*>(calloc(validityBuffSize, sizeof(uint64_t)));"
              )
          )
        },
        "for(int i = 0; i < left_out.size(); i++) { ",
        veInnerJoin.outputs.map { case NamedJoinExpression(outputName, veType, joinExpression) =>
          joinExpression.fold(ce => ce) match {
            case ex =>
              ex.isNotNullCode match {
                case None =>
                  CodeLines
                    .from(
                      s"${outputName}->data[i] = ${ex.cCode};",
                      s"$outputName->set_validity(i, 1);"
                    )
                    .indented
                case Some(nullCheck) =>
                  CodeLines
                    .from(
                      s"if( ${nullCheck} ) {",
                      CodeLines
                        .from(
                          s"${outputName}->data[i] = ${ex.cCode};",
                          s"$outputName->set_validity(i, 1);"
                        )
                        .indented,
                      "} else {",
                      CodeLines.from(s"$outputName->set_validity(i, 0);").indented,
                      "}"
                    )
                    .indented
              }
          }
        },
        "}",
        veInnerJoin.outputs.map { case NamedJoinExpression(outputName, veType, joinExpression) =>
          CodeLines.from(s"${outputName}->count = left_out.size();")
        }
      )
    )
  }
}
