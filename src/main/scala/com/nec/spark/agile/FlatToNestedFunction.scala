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

import com.nec.spark.agile.CFunctionGeneration.CExpression

import scala.annotation.tailrec

object FlatToNestedFunction {
  def nest(items: Seq[String], joiner: String): String = {

    @tailrec
    def go(remaining: List[String], joined: String, currentDepth: Int): String = {
      remaining match {
        case Nil => joined + (")" * (currentDepth - 1))
        case last :: Nil =>
          go(Nil, s"${joined}${last}", currentDepth + 1)
        case h :: rest if currentDepth == 0 =>
          go(rest, s"$joiner(${h}", currentDepth + 1)
        case h :: rest =>
          go(rest, s"${joined}, $joiner(${h}, ", currentDepth + 1)
      }
    }

    if (items.length == 2) s"${joiner}(${items(0)}, ${items(1)})" else go(items.toList, "", 0)
  }

  def runWhenNotNull(items: List[CExpression], function: String): CExpression = {
    items match {
      case Nil         => CExpression(cCode = "0", isNotNullCode = Some("0"))
      case item :: Nil => item
      case CExpression(cCode, None) :: CExpression(cCode2, None) :: Nil =>
        CExpression(cCode = s"${function}($cCode, $cCode2)", isNotNullCode = None)
      case CExpression(cCode, None) :: CExpression(cCode2, Some(notNull)) :: Nil =>
        CExpression(
          cCode = s"(${notNull}) ? (${function}($cCode, $cCode2)) : ${cCode}",
          isNotNullCode = None
        )
      case CExpression(cCode, Some(notNull)) :: CExpression(cCode2, None) :: Nil =>
        CExpression(
          cCode = s"(${notNull}) ? (${function}($cCode, $cCode2)) : ${cCode2}",
          isNotNullCode = None
        )
      case CExpression(cCode, Some(notNull)) :: CExpression(cCode2, Some(notNull2)) :: Nil =>
        CExpression(
          cCode =
            s"(${notNull} && ${notNull2}) ? (${function}($cCode, $cCode2)) : (${notNull} ? $cCode : $cCode2)",
          isNotNullCode = Some(s"${notNull} || ${notNull2}")
        )
      case other :: rest =>
        runWhenNotNull(items = other :: runWhenNotNull(rest, function) :: Nil, function = function)
    }
  }

}
