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

import io.sparkcyclone.spark.codegen.core.CodeLines
import io.sparkcyclone.spark.codegen.CFunctionGeneration.CExpression

sealed trait StringProducer extends Serializable {}

object StringProducer {
  trait FrovedisStringProducer extends StringProducer {
    def produce(output: String, count: String, index: String): CodeLines
  }

  def copyString(inputName: String): StringProducer = FrovedisCopyStringProducer(inputName)

  sealed trait CopyStringProducer {
    def inputName: String
  }

  final case class FrovedisCopyStringProducer(inputName: String) extends FrovedisStringProducer with CopyStringProducer {
    def produce(output: String, count: String, index: String): CodeLines = {
      s"${output}->move_assign_from(${inputName}->clone());"
    }
  }

  final case class StringChooser(condition: CExpression, trueval: String, falseval: String) extends FrovedisStringProducer {
    def produce(output: String, count: String, index: String): CodeLines = {
      CodeLines.scoped("Perform string choosing") {
        List(
          s"const auto condition = [&] (const size_t ${index}) -> bool { return ${condition.cCode}; };",
          s"""${output}->move_assign_from(nullable_varchar_vector::from_binary_choice(${count}, condition, "${trueval}", "${falseval}"));"""
        )
      }
    }
  }
}
