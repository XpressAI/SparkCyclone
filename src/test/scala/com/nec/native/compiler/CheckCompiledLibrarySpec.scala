package com.nec.native.compiler

import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.compiler.CppResource.CppResources
import com.nec.vectorengine.LibCyclone
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
