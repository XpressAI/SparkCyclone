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
package io.sparkcyclone.spark.codegen

import io.sparkcyclone.native.code._
import io.sparkcyclone.spark.codegen.StringProducer.FrovedisStringProducer
import org.apache.spark.sql.types._

/** Spark-free function evaluation */
object CFunctionGeneration {
  final case class CExpression(cCode: String, isNotNullCode: Option[String]) {
    def storeTo(outputName: String): CodeLines = isNotNullCode match {
      case None =>
        CodeLines
          .from(s"${outputName}->data[i] = ${cCode};", s"$outputName->set_validity(i, 1);")
          .indented
      case Some(nullCheck) =>
        CodeLines
          .from(
            s"if( ${nullCheck} ) {",
            CodeLines
              .from(s"${outputName}->data[i] = ${cCode};", s"$outputName->set_validity(i, 1);")
              .indented,
            "} else {",
            CodeLines.from(s"$outputName->set_validity(i, 0);").indented,
            "}"
          )
          .indented
    }
  }

  final case class TypedCExpression2(veType: VeScalarType, cExpression: CExpression)

  final case class NamedTypedCExpression(
    name: String,
    veType: VeScalarType,
    cExpression: CExpression
  ) {
    def cVector: CVector = veType.makeCVector(name)
  }

  final case class NamedStringExpression(name: String, stringProducer: StringProducer)

  sealed trait GroupByExpression {
    def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T
  }
  object GroupByExpression {
    final case class GroupByProjection(cExpression: CExpression) extends GroupByExpression {
      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenProj(
        cExpression
      )
    }
    final case class GroupByAggregation(aggregation: Aggregation) extends GroupByExpression {
      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenAgg(
        aggregation
      )
    }
  }

  trait Aggregation extends Serializable {
    def merge(prefix: String, inputPrefix: String): CodeLines
    def initial(prefix: String): CodeLines
    def partialValues(prefix: String): List[(CScalarVector, CExpression)]
    def iterate(prefix: String): CodeLines
    def compute(prefix: String): CodeLines
    def fetch(prefix: String): CExpression
    def free(prefix: String): CodeLines
  }

  final case class SuffixedAggregation(suffix: String, original: Aggregation) extends Aggregation {
    override def initial(prefix: String): CodeLines = original.initial(s"$prefix$suffix")

    override def iterate(prefix: String): CodeLines = original.iterate(s"$prefix$suffix")

    override def compute(prefix: String): CodeLines = original.compute(s"$prefix$suffix")

    override def fetch(prefix: String): CExpression = original.fetch(s"$prefix$suffix")

    override def free(prefix: String): CodeLines = original.free(s"$prefix$suffix")

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      original.partialValues(s"$prefix$suffix")

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      original.merge(s"$prefix$suffix", s"$inputPrefix$suffix")
  }

  abstract class DelegatingAggregation(val original: Aggregation) extends Aggregation {
    override def initial(prefix: String): CodeLines = original.initial(prefix)

    override def iterate(prefix: String): CodeLines = original.iterate(prefix)

    override def compute(prefix: String): CodeLines = original.compute(prefix)

    override def fetch(prefix: String): CExpression = original.fetch(prefix)

    override def free(prefix: String): CodeLines = original.free(prefix)

    override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
      original.partialValues(prefix)

    override def merge(prefix: String, inputPrefix: String): CodeLines =
      original.merge(prefix, inputPrefix)
  }

  object Aggregation {
    def sum(cExpression: CExpression): Aggregation = new Aggregation {
      override def initial(prefix: String): CodeLines =
        CodeLines.from(s"double ${prefix}_aggregate_sum = 0;")

      override def iterate(prefix: String): CodeLines =
        cExpression.isNotNullCode match {
          case None =>
            CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};")
          case Some(notNullCheck) =>
            CodeLines.from(
              s"if ( ${notNullCheck} ) {",
              CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};").indented,
              "}"
            )
        }

      override def fetch(prefix: String): CExpression =
        CExpression(s"${prefix}_aggregate_sum", None)

      override def free(prefix: String): CodeLines = CodeLines.empty

      override def compute(prefix: String): CodeLines = CodeLines.empty

      override def partialValues(prefix: String): List[(CScalarVector, CExpression)] =
        List(
          (
            CScalarVector(s"${prefix}_x", VeNullableDouble),
            CExpression(s"${prefix}_aggregate_sum", None)
          )
        )

      override def merge(prefix: String, inputPrefix: String): CodeLines =
        CodeLines.from(s"${prefix}_aggregate_sum += ${inputPrefix}_x->data[i];")
    }

    def avg(cExpression: CExpression): Aggregation = new Aggregation {
      override def initial(prefix: String): CodeLines =
        CodeLines.from(
          s"double ${prefix}_aggregate_sum = 0;",
          s"long ${prefix}_aggregate_count = 0;"
        )

      override def iterate(prefix: String): CodeLines =
        cExpression.isNotNullCode match {
          case None =>
            CodeLines.from(
              s"${prefix}_aggregate_sum += ${cExpression.cCode};",
              s"${prefix}_aggregate_count += 1;"
            )
          case Some(notNullCheck) =>
            CodeLines.from(
              s"if ( ${notNullCheck} ) {",
              CodeLines
                .from(
                  s"${prefix}_aggregate_sum += ${cExpression.cCode};",
                  s"${prefix}_aggregate_count += 1;"
                )
                .indented,
              "}"
            )
        }

      override def fetch(prefix: String): CExpression =
        CExpression(s"${prefix}_aggregate_sum / ${prefix}_aggregate_count", None)

      override def free(prefix: String): CodeLines = CodeLines.empty

      override def compute(prefix: String): CodeLines = CodeLines.empty

      override def partialValues(prefix: String): List[(CScalarVector, CExpression)] = List(
        (
          CScalarVector(s"${prefix}_aggregate_sum_partial_output", VeNullableDouble),
          CExpression(s"${prefix}_aggregate_sum", None)
        ),
        (
          CScalarVector(s"${prefix}_aggregate_count_partial_output", VeNullableLong),
          CExpression(s"${prefix}_aggregate_count", None)
        )
      )

      override def merge(prefix: String, inputPrefix: String): CodeLines =
        CodeLines.from(
          s"${prefix}_aggregate_sum += ${inputPrefix}_aggregate_sum_partial_output->data[i];",
          s"${prefix}_aggregate_count += ${inputPrefix}_aggregate_count_partial_output->data[i];"
        )
    }
  }

  val KeyHeaders = CodeLines.from(
    """#include "cyclone/cyclone.hpp"""",
    """#include "frovedis/dataframe/join.hpp"""",
    """#include "frovedis/core/set_operations.hpp"""",
    """#include "frovedis/text/datetime_utility.hpp"""",
    "#include <math.h>",
    "#include <stddef.h>",
    "#include <bitset>",
    "#include <iostream>",
    "#include <string>",
    "#include <tuple>",
    "#include <vector>"
  )

  final case class CFunction(
    inputs: Seq[CVector],
    outputs: Seq[CVector],
    body: CodeLines,
    hasSets: Boolean = false
  ) {
    def toCodeLinesSPtr(functionName: String): CodeLines = CodeLines.from(
      """#include "cyclone/cyclone.hpp"""",
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/core/set_operations.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/text/datetime_utility.hpp"""",
      """#include "frovedis/text/dict.hpp"""",
      "#include <math.h>",
      "#include <stddef.h>",
      "#include <bitset>",
      "#include <iostream>",
      "#include <string>",
      "#include <tuple>",
      "#include <vector>",
      toCodeLinesNoHeaderOutPtr2(functionName)
    )

    def toCodeLinesS(functionName: String): CodeLines = CodeLines.from(
      """#include "cyclone/cyclone.hpp"""",
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/core/set_operations.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/text/datetime_utility.hpp"""",
      """#include "frovedis/text/dict.hpp"""",
      "#include <math.h>",
      "#include <stddef.h>",
      "#include <bitset>",
      "#include <iostream>",
      "#include <string>",
      "#include <tuple>",
      "#include <vector>",
      toCodeLinesNoHeader(functionName)
    )

    def arguments: Seq[CVector] = inputs ++ outputs

    // USED
    def toCodeLinesNoHeader(functionName: String): CodeLines = {
      CodeLines.from(
        s"""extern "C" long $functionName(""",
        arguments
          .map { cVector =>
            s"${cVector.veType.cVectorType} *${cVector.name}"
          }
          .mkString(",\n"),
        ") {",
        body.indented,
        "  ",
        "  return 0;",
        "};"
      )
    }

    // USED in tests
    def toCodeLinesHeaderPtr(functionName: String): CodeLines = {
      CodeLines.from(
        KeyHeaders,
        s"""extern "C" long $functionName(""", {
          inputs
            .map { cVector =>
              s"${cVector.veType.cVectorType} **${cVector.name}"
            } ++ { if (hasSets) List("int *sets") else Nil } ++
            outputs
              .map { cVector =>
                s"${cVector.veType.cVectorType} **${cVector.name}"
              }
        }
          .mkString(",\n"),
        ") {",
        body.indented,
        "  ",
        "  return 0;",
        "};"
      )
    }

    // USED
    def toCodeLinesNoHeaderOutPtr2(functionName: String): CodeLines = {
      CodeLines.from(
        s"""extern "C" long $functionName(""", {
          List(
            inputs
              .map { cVector =>
                s"${cVector.veType.cVectorType} **${cVector.name}_m"
              },
            if (hasSets) List("int *sets") else Nil,
            outputs
              .map { cVector =>
                s"${cVector.veType.cVectorType} **${cVector.name}_mo"
              }
          ).flatten
        }
          .mkString(",\n"),
        ") {",
        CodeLines
          .from(
            inputs.map { cVector =>
              CodeLines.from(
                s"${cVector.veType.cVectorType} *${cVector.name} = ${cVector.name}_m[0];"
              )
            },
            outputs.map { cVector =>
              CodeLines.from(
                s"${cVector.veType.cVectorType} *${cVector.name} = ${cVector.veType.cVectorType}::allocate();",
                s"*${cVector.name}_mo = ${cVector.name};"
              )
            },
            "",
            body
          )
          .indented,
        "  ",
        "  return 0;",
        "};"
      )
    }
  }
}
