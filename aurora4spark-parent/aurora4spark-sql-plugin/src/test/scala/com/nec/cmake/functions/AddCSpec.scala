package com.nec.cmake.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import com.nec.arrow.{CArrowNativeInterfaceNumeric, TransferDefinitions}
import com.nec.arrow.functions.{Add, Sum}
import com.nec.cmake.CMakeBuilder
import com.nec.older.SumPairwise
import org.scalatest.freespec.AnyFreeSpec

final class AddCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val firstColumnNumbers: Seq[Seq[Double]] = Seq(Seq(1.0, 2.0, 3.0, 4.0, 5.0))
    val secondColumnNumbers: Seq[Seq[Double]] = Seq(Seq(10.0, 20.0, 30.0, 40.0, 50.0))

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Add.PairwiseSumCode)
        .mkString("\n\n")
    )

    withArrowFloat8Vector(firstColumnNumbers) { firstColumn => {
      withArrowFloat8Vector(secondColumnNumbers) { secondColumn =>
        val pairwiseSum = Add.runOn(new CArrowNativeInterfaceNumeric(cLib.toString))(firstColumn, secondColumn)
        val jvmSum = Add.addJVM(firstColumn, secondColumn)

        assert(pairwiseSum == jvmSum)
      }
    }

    }
  }

}
