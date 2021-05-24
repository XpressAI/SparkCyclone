package com.nec.spark

import com.nec.spark.WordCountSpec.withArrowStringVector
import com.nec.WordCount
import org.apache.arrow.vector.{FieldVector, VarCharVector}
import org.scalatest.freespec.AnyFreeSpec

import java.util

object WordCountSpec {
  val schema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(
    """{"fields": [{"name": "value", "nullable" : true, "type": {"name": "utf8"}, "children": []}]}"""
  )
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
final class WordCountSpec extends AnyFreeSpec {
  "JVM word count works" in {
    withArrowStringVector(Seq("hello", "test", "hello")) { vcv =>
      assert(WordCount.wordCountJVM(vcv) == Map("hello" -> 2, "test" -> 1))
    }
  }
}
