import CountArrowStringsSpec.schema
import CountStringsVESpec.Sample.someStrings
import com.nec.CountStringsLibrary.{data_out, unique_position_counter}
import com.nec.WordCount
import com.nec.WordCount.count_strings
import com.sun.jna.{Library, Pointer}
import org.apache.arrow.vector.{BaseVariableWidthVector, FieldVector, VarCharVector}
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

}

final class CountStringsCSpec extends AnyFreeSpec {

  "It works" in {
    val ss = CountStringsVESpec.Sample
    val result = ss.computex86(CBuilder.buildC(WordCount.SourceCode))
    info(s"Got: $result")
    assert(result == ss.expectedWordCount)
  }

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

  "Through Arrow, it works" in {
    val ss = CountStringsVESpec.Sample
    val cLib = CBuilder.buildC(WordCount.SourceCode)

    def wordCountArrow(libPath: Path, varCharVector: VarCharVector): Map[String, Int] = {
      // will abstract this out later
      import scala.collection.JavaConverters._
      val thingy2 =
        new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
      val nl = thingy2.getNativeLibrary
      val fn = nl.getFunction(count_strings)
      val dc = new data_out.ByReference()
      fn.invokeInt(
        Array[java.lang.Object](
          varCharVector.getDataBuffer.nioBuffer(),
          varCharVector.getOffsetBuffer.nioBuffer(), {

            (0 until varCharVector.getValueCount).map { i =>
              varCharVector.getOffsetBuffer.getInt(
                (i + 1).toLong * BaseVariableWidthVector.OFFSET_WIDTH
              ) - varCharVector.getStartOffset(i)

            }.toArray
          },
          java.lang.Integer.valueOf(varCharVector.getValueCount),
          dc
        )
      )

      /**
       * It may be quite smart to return back a vector that can directly become
       * a VarCharVector as well!
       *
       * Then, we have an Aveo which takes VarCharVector => VarCharVector + IntVector or something similar.
       */

      val counted_strings = dc.logical_total.toInt
      assert(counted_strings == someStrings.toSet.size)

      val results =
        (0 until counted_strings).map { i =>
          new unique_position_counter(new Pointer(Pointer.nativeValue(dc.data) + i * 8))
        }
      results.map { unique_position_counter =>
        someStrings(unique_position_counter.string_i) -> unique_position_counter.count
      }.toMap
    }

    withArrowStringVector(ss.strings) { vector =>
      assert(wordCountArrow(cLib, vector) == ss.expectedWordCount)
    }
  }

}
