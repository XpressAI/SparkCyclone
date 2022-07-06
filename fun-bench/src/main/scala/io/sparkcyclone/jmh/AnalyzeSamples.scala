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
package io.sparkcyclone.jmh

import StackSamplingProfiler.ThreadsSamples
import cats.effect.IO

object AnalyzeSamples {
  val IgnoreFiles = Set("Object.java", "Unsafe.java", "ThreadImpl.java")
  val TopN = 10
  def apply(threadsSamples: ThreadsSamples): IO[String] = IO.blocking {
    threadsSamples
      .flatMap(threadsSample =>
        threadsSample.threads.filter(_.stack.nonEmpty).flatMap(_.stack.headOption)
      )
      .filterNot(_.fileName.exists(f => IgnoreFiles.contains(f)))
      .groupBy(identity)
      .mapValues(_.size)
      .toList
      .sortBy(_._2)
      .reverse
      .take(TopN)
      .map { case (element, count) =>
        s"${count} ${element.fileName.map(f => f + ":" + element.lineNumber).mkString} ${element.className}#${element.methodName}"
      }
      .mkString("\n")
  }
}
