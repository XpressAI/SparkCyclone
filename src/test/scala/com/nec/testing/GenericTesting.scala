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
