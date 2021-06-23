package com.nec.arrow.functions

import com.nec.arrow.ArrowVectorBuilders.{withDirectFloat8Vector, withDirectIntVector}
import com.nec.ve.JoinVeSpec.Join
import org.scalatest.freespec.AnyFreeSpec

final class ArrowJVMJoinSpec extends AnyFreeSpec {
  "JVM sum works" in {
    val firstColumn: Seq[Double] =
      Seq(10.0, 20.0, 30.0, 40.0, 50.0)
    val secondColumn: Seq[Double] =
      Seq(100.0, 200.0, 300.0, 400.0, 500.0)
    val firstKey: Seq[Int] =
      Seq(1, 2, 3, 4, 5)
    val secondKey: Seq[Int] =
      Seq(5, 200, 800, 3, 1)

    withDirectFloat8Vector(firstColumn) { firstColumnVec =>
      withDirectFloat8Vector(secondColumn) { secondColumnCec =>
        withDirectIntVector(firstKey) { firstKeyVec =>
          withDirectIntVector(secondKey) { secondKeyVec =>

            assert(
              Join.joinJVM(firstColumnVec, secondColumnCec, firstKeyVec, secondKeyVec) ==
                Seq((10.0, 500.0), (30.0, 400.0), (50.0, 100.0))
            )
          }
        }
      }
    }
  }
}
