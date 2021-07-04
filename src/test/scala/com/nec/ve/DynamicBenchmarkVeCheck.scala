package com.nec.ve

import com.nec.spark.BenchTestingPossibilities
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import org.scalatest.BeforeAndAfterAll

final class DynamicBenchmarkVeCheck
  extends AnyFreeSpec
  with BeforeAndAfterAll
  with BenchTestAdditions {

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isVE).foreach(runTestCase)

  override protected def afterAll(): Unit = {
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
    super.afterAll()
  }
}
