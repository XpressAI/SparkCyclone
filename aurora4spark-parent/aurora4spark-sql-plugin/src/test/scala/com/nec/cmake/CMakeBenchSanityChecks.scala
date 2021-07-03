package com.nec.cmake
import com.nec.spark.BenchTestingPossibilities.BenchTestAdditions
import com.nec.spark.BenchTestingPossibilities
import org.scalatest.freespec.AnyFreeSpec

final class CMakeBenchSanityChecks extends AnyFreeSpec with BenchTestAdditions {

  BenchTestingPossibilities.possibilities.filter(_.testingTarget.isCMake).foreach(runTestCase)

}
