import CountArrowStringsSpec.schema
import CountStringsCSpec.withArrowStringVector
import com.nec.WordCount
import com.nec.WordCount.wordCountArrowCC
import org.apache.arrow.vector.{FieldVector, VarCharVector}
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.{Path, Paths}
import java.util
import scala.sys.process._

object CountStringsCSpec {

  lazy val CMakeListsTXT: Path = Paths
    .get(
      this.getClass
        .getResource("/CMakeLists.txt")
        .toURI
    )
    .toAbsolutePath

  def withArrowStringVector[T](stringBatch: Seq[String])(f: VarCharVector => T): T = {
    import org.apache.arrow.memory.RootAllocator
    import org.apache.arrow.vector.VectorSchemaRoot
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    try {
      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        val root = new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), 2)
        stringBatch.view.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
        }
        vcv.setValueCount(stringBatch.length)
        root.setRowCount(stringBatch.length)
        f(vcv)
      } finally vcv.close()
    } finally alloc.close()
  }
}

final class CountStringsCSpec extends AnyFreeSpec {

  "Through Arrow, it works" in {
    val ss = CountStringsVESpec.Sample
    val cLib = CBuilder.buildC(WordCount.SourceCode)

    withArrowStringVector(ss.strings) { vector =>
      assert(wordCountArrowCC(cLib, vector) == ss.expectedWordCount)
    }
  }

}
