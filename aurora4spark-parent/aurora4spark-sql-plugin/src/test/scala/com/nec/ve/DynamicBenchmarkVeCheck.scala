package com.nec.ve

import com.nec.spark.BenchTestingPossibilities
import com.nec.testing.Testing.DataSize
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.BeforeAndAfterAll
import com.nec.spark.Aurora4SparkExecutorPlugin

final class DynamicBenchmarkVeCheck extends AnyFreeSpec with BeforeAndAfterAll {
  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isVE).foreach { testing =>
    testing.name.value in {
      // enable this in case of any segfaults
      // println(s"Running: ${testing.name}")
      val sparkSession = testing.prepareSession(dataSize = DataSize.SanityCheckSize)
      try testing.verify(sparkSession)
      finally testing.cleanUp(sparkSession)
    }
  }
  override protected def afterAll(): Unit = {
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
    super.afterAll()
  }
}
