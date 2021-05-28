package com.nec.cmake.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.Avg
import com.nec.cmake.CMakeBuilder
import org.scalatest.freespec.AnyFreeSpec

final class AvgCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val ss: Seq[Seq[Double]] = Seq(Seq(4, 2, 3), Seq(4, 1, 13), Seq(4, 2, 43), Seq(4, 3, 9))
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Avg.AvgSourceCode)
        .mkString("\n\n")
    )

    withArrowFloat8Vector(ss) { vector =>
      assert(Avg.runOn(new CArrowNativeInterfaceNumeric(cLib))(vector, 3) == Avg.avgJVM(vector, 3))
    }
  }

}
