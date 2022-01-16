package com.nec.ve

import com.nec.spark.agile.CppResource.CppResources
import com.nec.ve.VeKernelCompiler.PlatformLibrarySoName
import org.scalatest.freespec.AnyFreeSpec

final class CheckCompiledLibrarySpec extends AnyFreeSpec {
  "We can get the resource file" in {
    val names = CppResources.AllVe.all.map(_.name)
    assert(names.contains(PlatformLibrarySoName))
  }
}
