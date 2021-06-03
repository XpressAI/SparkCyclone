package com.nec.cmake.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.functions.Sum
import com.nec.cmake.CMakeBuilder
import org.scalatest.freespec.AnyFreeSpec

final class SumCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val ss: Seq[Seq[Double]] = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Sum.SumSourceCode)
        .mkString("\n\n")
    )

    withArrowFloat8Vector(ss) { vector =>
      assert(
        Sum.runOn(new CArrowNativeInterfaceNumeric(cLib.toString))(vector, 3) == Sum
          .sumJVM(vector, 3)
      )
    }
  }

}
