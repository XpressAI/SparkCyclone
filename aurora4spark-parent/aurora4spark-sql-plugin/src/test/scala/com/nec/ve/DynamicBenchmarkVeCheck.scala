package com.nec.ve

import com.nec.spark.BenchTestingPossibilities
import com.nec.testing.Testing.DataSize
import org.scalatest.freespec.AnyFreeSpec

final class DynamicBenchmarkVeCheck extends AnyFreeSpec {
  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isVE).foreach { testing =>
    testing.name.value in {
      println(s"Running: ${testing.name}")
      val sparkSession = testing.prepareSession(dataSize = DataSize.SanityCheckSize)
      try testing.verify(sparkSession)
      finally testing.cleanUp(sparkSession)
    }
  }
}
