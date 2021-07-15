package com.nec.cmake.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import com.nec.arrow.CArrowNativeInterfaceNumeric
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.functions.AddPairwise
import com.nec.cmake.CMakeBuilder
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class AddCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val firstColumnNumbers: Seq[Seq[Double]] = Seq(Seq(1.0, 2.0, 3.0, 4.0, 5.0))
    val secondColumnNumbers: Seq[Seq[Double]] = Seq(Seq(10.0, 20.0, 30.0, 40.0, 50.0))

    val cLib = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, AddPairwise.PairwiseSumCode)
        .mkString("\n\n")
    )

    withArrowFloat8Vector(firstColumnNumbers) { firstColumn =>
      {
        withArrowFloat8Vector(secondColumnNumbers) { secondColumn =>
          WithTestAllocator { alloc =>
            val outVector = new Float8Vector("value", alloc)
            AddPairwise.runOn(new CArrowNativeInterfaceNumeric(cLib.toString))(
              firstColumn,
              secondColumn,
              outVector
            )

            val pairwiseSum = (0 until outVector.getValueCount).map(outVector.get).toList

            val jvmSum = AddPairwise.addJVM(firstColumn, secondColumn).toList

            assert(pairwiseSum == jvmSum)
          }
        }
      }

    }
  }

}
