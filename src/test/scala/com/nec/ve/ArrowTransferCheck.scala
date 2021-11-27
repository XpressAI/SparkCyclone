package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.arrow.WithTestAllocator
import com.nec.ve.VeColBatch.VeColVector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowTransferCheck extends AnyFreeSpec with WithVeProcess {
  "It works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
        val arrowVec = colVec.toArrowVector()

        expect(arrowVec.toString == f8v.toString)
      }
    }
  }
}
