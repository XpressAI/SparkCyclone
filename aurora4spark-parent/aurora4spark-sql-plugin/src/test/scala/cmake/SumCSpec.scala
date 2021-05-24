package cmake

import cmake.SumCSpec.withArrowFloat8Vector
import com.nec.Sum
import com.nec.native.CArrowNativeInterfaceNumeric
import com.nec.native.TransferDefinitions
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

object SumCSpec {

  def withArrowFloat8Vector[T](inputColumns: Seq[Seq[Double]])(f: Float8Vector => T): T = {
    import org.apache.arrow.memory.RootAllocator
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    val data = inputColumns.flatten
    try {
      val vcv = new Float8Vector("value", alloc)
      vcv.allocateNew()
      try {
        inputColumns.flatten.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str)
        }
        vcv.setValueCount(data.size)

        f(vcv)
      } finally vcv.close()
    } finally alloc.close()
  }
}

final class SumCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val ss: Seq[Seq[Double]] = Seq(
      Seq(1, 2, 3),
      Seq(4, 5, 6)
    )
    val cLib = CBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, Sum.SumSourceCode)
        .mkString("\n\n")
    )

    withArrowFloat8Vector(ss) { vector =>
      assert(
        Sum.runOn(new CArrowNativeInterfaceNumeric(cLib))(vector, 3) == Sum.sumJVM(vector, 3)
      )
    }
  }

}
