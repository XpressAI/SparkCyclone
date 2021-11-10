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
import com.nec.arrow.ArrowNativeInterface
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.VeArrowNativeInterface
import com.nec.arrow.functions.CsvParse
import com.nec.testing.NativeCSVParserBenchmark.ParserTestState
import com.nec.testing.NativeCSVParserBenchmark.SimpleTestType
import com.nec.testing.Testing.TestingTarget
import com.nec.ve.VeKernelCompiler
import com.nec.ve.VeKernelCompiler.compile_cpp
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector
import com.eed3si9n.expecty.Expecty._
import com.nec.cmake.functions.ParseCSVSpec
import com.nec.cmake.functions.ParseCSVSpec.inTolerance
import com.nec.native.NativeEvaluator.CNativeEvaluator
import org.bytedeco.veoffload.global.veo

import java.nio.file.Files

object NativeCSVParserBenchmark {
  sealed trait SimpleTestType {
    def testingTarget: TestingTarget
  }
  object SimpleTestType {
    case object CBased extends SimpleTestType {
      override def toString: String = "_CMake"
      override def testingTarget: TestingTarget = TestingTarget.CMake
    }
    case object VEBased extends SimpleTestType {
      override def toString: String = "_VE"
      override def testingTarget: TestingTarget = TestingTarget.VectorEngine
    }
  }
  trait ParserTestState {
    def originalArray: Array[Array[Double]]
    def interface: ArrowNativeInterface
    def bufferAllocator: BufferAllocator
    def close(): Unit
    def string: String
  }
  private val MEGABYTE = 1024 * 1024
  final case class Megs(value: Int) {
    def bytes: Int = value * MEGABYTE
  }
  object Megs {
    val Default = Megs(150)
  }
}

final case class NativeCSVParserBenchmark(
  simpleTestType: SimpleTestType,
  dataSize: NativeCSVParserBenchmark.Megs = NativeCSVParserBenchmark.Megs.Default
) extends GenericTesting {
  override type State = ParserTestState
  override def benchmark(state: State): Unit = {
    val a = new Float8Vector("a", state.bufferAllocator)
    val b = new Float8Vector("b", state.bufferAllocator)
    val c = new Float8Vector("c", state.bufferAllocator)
    CsvParse.runOn(state.interface)(Right(state.string), a, b, c)

    val randomRow = scala.util.Random.nextInt(state.originalArray.length)
    val randomCol = scala.util.Random.nextInt(3)

    val theCell = state.originalArray.apply(randomRow).apply(randomCol)
    val expectedCell = List(a, b, c)
      .apply(randomCol)
      .get(randomRow)
    assert(inTolerance(theCell, expectedCell))
  }

  override def cleanUp(state: State): Unit = {}
  override def testingTarget: Testing.TestingTarget = simpleTestType.testingTarget
  override def init(): State = {
    val minimum = dataSize.bytes
    val arrItems = scala.collection.mutable.Buffer.empty[Array[Double]]
    val stringBuilder = new StringBuilder()
    stringBuilder ++= "a,b,c\n"
    while (stringBuilder.size < minimum) {
      val line = (
        scala.util.Random.nextDouble(),
        scala.util.Random.nextDouble(),
        scala.util.Random.nextDouble()
      )
      arrItems += Array(line._1, line._2, line._3)
      stringBuilder ++= (ParseCSVSpec.renderLine(line) + "\n")
    }
    val inputString = stringBuilder.toString()
    val inputArray = arrItems.toArray
    arrItems.clear()
    simpleTestType match {
      case SimpleTestType.CBased =>
        new ParserTestState {
          val bufferAllocator = new RootAllocator(Integer.MAX_VALUE)
          val interface = CNativeEvaluator.forCode(CsvParse.CsvParseCode)
          override def close(): Unit = ()
          override def string: String = inputString

          override def originalArray: Array[Array[Double]] = inputArray
        }
      case SimpleTestType.VEBased =>
        new ParserTestState {
          override def string: String = inputString
          val bufferAllocator = new RootAllocator(Integer.MAX_VALUE)
          val tmpBuildDir = Files.createTempDirectory("ve-spark-tmp")
          val soName = compile_cpp(
            buildDir = tmpBuildDir,
            config = VeKernelCompiler.VeCompilerConfig.testConfig,
            List(TransferDefinitions.TransferDefinitionsSourceCode, CsvParse.CsvParseCode).mkString(
              "\n\n"
            )
          ).toAbsolutePath.toString
          val proc = veo.veo_proc_create(0)

          val lib = veo.veo_load_library(proc, soName)
          require(lib != 0, s"Expected lib != 0, got ${lib}")

          val interface =
            new VeArrowNativeInterface(proc, lib)

          def close(): Unit = {
            veo.veo_proc_destroy(proc)
          }

          override def originalArray: Array[Array[Double]] = inputArray
        }
    }
  }
}
