package io.sparkcyclone.native.compiler

import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.native.compiler.CppResource.CppResources
import io.sparkcyclone.vectorengine.LibCyclone
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class CheckCompiledLibrarySpec extends AnyWordSpec {
  "CppResources" should {
    s"be able to fetch the resource '${LibCyclone.FileName}'" in {
      CppResources.AllVe.all.map(_.name) should contain (LibCyclone.FileName)
    }
  }
}
