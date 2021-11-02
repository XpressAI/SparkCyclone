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
import java.util.regex.Pattern

/** Compiler-friendly name that we can use as part of class an method names. */
final case class CleanName(value: String) {
  override def toString: String = value
}
object CleanName {

  implicit class RichStringClean(string: String) {
    def clean: CleanName = fromString(string)
    def cleanName: CleanName = fromString(string)
  }
  def fromString(value: String): CleanName = CleanName(
    value
      .replaceAll(Pattern.quote("-"), "_minus_")
      .replaceAll(Pattern.quote("+"), "_plus_")
      .replaceAll("[^a-zA-Z_0-9]", "_")
      .replaceAll("_+", "_")
      .replaceAll("[^a-zA-Z_0-9]", "")
  )
}
