package com.nec.spark.agile

import com.nec.spark.agile.CppResource.CppResources
import com.nec.spark.agile.ListCppResourcesSpec.LowerBound
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Files

object ListCppResourcesSpec {
  val LowerBound = CppResource("cpp/frovedis/core/lower_bound.hpp")
}

final class ListCppResourcesSpec extends AnyFreeSpec {

  "It lists lower_bound.hpp" in {
    com.eed3si9n.expecty.Expecty.assert(CppResources.All.all.contains(LowerBound))
  }

  "A resource can be copied" in {
    val tempDir = Files.createTempDirectory("tst")
    val expectedFile = tempDir.resolve(LowerBound.name)
    LowerBound.copyTo(tempDir)
    assert(Files.exists(expectedFile))
  }

}
