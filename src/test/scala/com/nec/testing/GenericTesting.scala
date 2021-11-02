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
package com.nec.testing

import com.nec.spark.agile
import com.nec.spark.agile.CleanName
import com.nec.testing.Testing.TestingTarget

abstract class GenericTesting { this: Product =>
  type State
  def init(): State
  def benchmark(state: State): Unit
  def cleanUp(state: State): Unit
  final def name: CleanName = agile.CleanName.fromString(this.toString + s"_${testingTarget.label}")
  def testingTarget: TestingTarget
}

object GenericTesting {

  val possibilities: List[GenericTesting] = List(
    NativeCSVParserBenchmark(NativeCSVParserBenchmark.SimpleTestType.CBased),
    NativeCSVParserBenchmark(NativeCSVParserBenchmark.SimpleTestType.VEBased)
  )
  val possibilitiesMap: Map[String, GenericTesting] =
    possibilities.map(gt => gt.name.value -> gt).toMap

}
