package com.nec.cmake.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowStringVector
import com.nec.arrow.functions.WordCount.runOn
import com.nec.arrow.CArrowNativeInterface
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.WordCount
import com.nec.cmake.CMakeBuilder
import com.nec.ve.CountStringsVESpec
import org.scalatest.freespec.AnyFreeSpec

final class CountStringsCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val ss = CountStringsVESpec.Sample
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, WordCount.WordCountSourceCode)
        .mkString("\n\n")
    )

    withArrowStringVector(ss) { vector =>
      assert(runOn(new CArrowNativeInterface(cLib))(vector) == WordCount.wordCountJVM(vector))
    }
  }

}
