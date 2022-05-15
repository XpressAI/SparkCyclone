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
package com.nec.ve

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.{BenchTestingPossibilities, SparkCycloneExecutorPlugin}
import com.nec.vectorengine.VeProcess
import org.apache.log4j.{Level, Logger}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

@VectorEngineTest
final class DynamicBenchmarkVeCheck
  extends AnyFreeSpec
  with BeforeAndAfterAll
  with BenchTestAdditions {

  override def beforeAll: Unit = {
    super.beforeAll
    Logger.getRootLogger.setLevel(Level.INFO)
    SparkCycloneExecutorPlugin.veProcess = VeProcess.create(-1, getClass.getName)
  }

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isVE).foreach(runTestCase)

  override def afterAll(): Unit = {
    SparkCycloneExecutorPlugin.veProcess.close
    super.afterAll
  }
}
