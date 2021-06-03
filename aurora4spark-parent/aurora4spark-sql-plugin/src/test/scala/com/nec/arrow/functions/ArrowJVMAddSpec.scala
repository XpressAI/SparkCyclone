package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMAddSpec extends AnyFreeSpec {
  "JVM add works" in {
    val firstInputColumn: Seq[Seq[Double]] =
      Seq(Seq(1.0, 2.0, 3.0, 10.0, 20.0, 30.0, 50.0, 60.0, 70.0))
    val secondInputColumn: Seq[Seq[Double]] =
      Seq(Seq(5.0, 7.0, 8.0, 15.0, 25.0, 35.0, 150.0, 260.0, 370.0))

    withArrowFloat8Vector(firstInputColumn) { firstVector =>
      withArrowFloat8Vector(secondInputColumn) { secondVector =>
        assert(
          Add.addJVM(firstVector, secondVector) ==
            Seq(6.0, 9.0, 11.0, 25.0, 45.0, 65.0, 200.0, 320.0, 440.0)
        )
      }
    }
  }
}
