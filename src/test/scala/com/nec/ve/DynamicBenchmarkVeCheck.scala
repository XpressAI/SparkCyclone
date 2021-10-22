package com.nec.ve

import com.nec.spark.BenchTestingPossibilities
import org.scalatest.freespec.AnyFreeSpec
import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ConfigMap
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.bytedeco.veoffload.global.veo

final class DynamicBenchmarkVeCheck
  extends AnyFreeSpec
  with BeforeAndAfterAll
  with BenchTestAdditions {

  override protected def beforeAll(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.INFO)
    Aurora4SparkExecutorPlugin._veo_proc = veo.veo_proc_create(-1)
    super.beforeAll()
  }

  /** TODO We could also generate Spark plan details from here for easy cross-referencing, as well as codegen */
  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isVE).foreach(runTestCase)

  override protected def afterAll(): Unit = {
    Aurora4SparkExecutorPlugin.closeProcAndCtx()
    super.afterAll()
  }
}
