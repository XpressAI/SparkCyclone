import java.nio.file.{Path, Paths}

import SumCSpec.withArrowFloat8Vector
import com.nec.Sum
import com.nec.native.{CArrowNativeInterfaceNumeric, TransferDefinitions}
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

object SumCSpec {
  val schema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(
    """{"fields": [{"name": "value", "nullable" : true, "type": {"name": "utf8"}, "children": []}]}"""
  )
  lazy val CMakeListsTXT: Path = Paths
    .get(
      this.getClass
        .getResource("/CMakeLists.txt")
        .toURI
    )
    .toAbsolutePath

  def withArrowFloat8Vector[T](stringBatch: Seq[Seq[Double]])(f: Float8Vector => T): T = {
    import org.apache.arrow.memory.RootAllocator
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    val data = stringBatch.flatten
    try {
      val vcv = new Float8Vector("value", alloc)
      vcv.allocateNew()
      try {
        stringBatch.flatten.zipWithIndex.foreach { case (str, idx) =>
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
