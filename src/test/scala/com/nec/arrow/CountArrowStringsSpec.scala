package com.nec.arrow

import com.nec.arrow.CountArrowStringsSpec.schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BaseVariableWidthVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.scalatest.freespec.AnyFreeSpec

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util
import java.util.UUID

object CountArrowStringsSpec {
  val schema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(
    """{"fields": [{"name": "value", "nullable" : true, "type": {"name": "utf8"}, "children": []}]}"""
  )
}

final class CountArrowStringsSpec extends AnyFreeSpec {

  def stringsToArrow(strings: String*): Array[Byte] = {
    import org.apache.arrow.vector.VectorSchemaRoot
    WithTestAllocator { alloc =>
      val baos = new ByteArrayOutputStream()

      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        val root = new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), 2)
        val arrowStreamWriter = new ArrowStreamWriter(root, null, baos)
        arrowStreamWriter.start()
        strings.grouped(5).foreach { stringBatch =>
          stringBatch.view.zipWithIndex.foreach { case (str, idx) =>
            vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
          }
          vcv.setValueCount(stringBatch.length)
          root.setRowCount(stringBatch.length)
          arrowStreamWriter.writeBatch()
        }
        arrowStreamWriter.end()
        baos.toByteArray
      } finally vcv.close()
    }
  }

  "We can get a Byte Array which contains both the Strings" in {
    val string1 = ('A' to 'Z').mkString
    val string2 = "XAY"
    assert(stringsToArrow(string1, string2).containsSlice(string1.getBytes("UTF-8")))
    assert(stringsToArrow(string1, string2).containsSlice(string2.getBytes("UTF-8")))
  }

  def readStrings(bytes: Array[Byte]): List[String] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    val root = arrowStreamReader.getVectorSchemaRoot()

    Iterator
      .continually {
        if (!arrowStreamReader.loadNextBatch()) None
        else
          Option {
            import scala.collection.JavaConverters._

            root.getFieldVectors.asScala
              .collect { case vc: VarCharVector =>
                (0 until root.getRowCount).map(i => new String(vc.get(i), "utf8"))
              }
              .toList
              .flatten
          }
      }
      .takeWhile(_.isDefined)
      .flatten
      .toList
      .flatten
  }

  "We can retrieve the Strings back from a raw Byte Array" in {
    info("We do it in Java so that we can then do it in C")

    assert(readStrings(stringsToArrow("ABC", "DEF")) == List("ABC", "DEF"))
  }

  def generateCleanString(length: Int): String = {
    UUID.randomUUID().toString.take(length)
  }

  "Generate a random String" in {
    info(generateCleanString(19))
  }

  def makeStrings(size: Int): List[String] = {
    List
      .fill(size)(generateCleanString(Math.abs(scala.util.Random.nextInt(100))))
  }

  "We can retrieve many Strings back from a raw Byte Array" in {
    val Size = 2000
    val input = makeStrings(Size)
    val gotResult = readStrings(stringsToArrow(input: _*))
    assert(gotResult.size == Size)
    assert(gotResult == input)
  }

  case class StringInfo(startAddr: Long, position: Int, length: Int, value: String)

  def readStringPositionsValuesLengths(bytes: Array[Byte]): List[StringInfo] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val arrowStreamReader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    val root = arrowStreamReader.getVectorSchemaRoot

    Iterator
      .continually {
        if (!arrowStreamReader.loadNextBatch()) None
        else
          Option {
            import scala.collection.JavaConverters._

            root.getFieldVectors.asScala
              .collect { case vc: VarCharVector =>
                (0 until root.getRowCount).map { i =>
                  val startOffset = vc
                    .getOffsetBuffer()
                    .getInt(i * BaseVariableWidthVector.OFFSET_WIDTH)

                  val dataLength = vc.getOffsetBuffer.getInt(
                    (i + 1) * BaseVariableWidthVector.OFFSET_WIDTH
                  ) - startOffset
                  val result = new Array[Byte](dataLength)
                  vc.getDataBuffer.getBytes(startOffset, result, 0, dataLength)
                  StringInfo(
                    startAddr = vc.getDataBuffer.memoryAddress(),
                    position = startOffset,
                    length = dataLength,
                    value = new String(result, "utf8")
                  )
                }
              }
              .toList
              .flatten
          }
      }
      .takeWhile(_.isDefined)
      .flatten
      .toList
      .flatten
  }

  "We can retrieve String positions from the byte array" in {
    val byteArray = stringsToArrow("ABCG", "DEF")
    assert(
      readStringPositionsValuesLengths(byteArray).map(_.copy(startAddr = -1)) == List(
        StringInfo(startAddr = -1, position = 0, length = 4, value = "ABCG"),
        StringInfo(startAddr = -1, position = 4, length = 3, value = "DEF")
      )
    )
  }

  /** Ignored as very verbose and does not assert anything */
  "We can get info for a longer set of Strings" ignore {
    val someStrings =
      readStringPositionsValuesLengths(stringsToArrow(makeStrings(100): _*)).toList.take(1)
    someStrings.foreach { strInfo => info(strInfo.toString) }
  }

  def writeAndGet(stringBatch: String*): List[StringInfo] = {

    import org.apache.arrow.vector.VectorSchemaRoot
    WithTestAllocator { alloc =>
      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        val root =
          new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), stringBatch.length)
        stringBatch.view.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
        }
        vcv.setValueCount(stringBatch.length)
        val vc = vcv
        (0 until root.getRowCount).map { i =>
          val startOffset = vc
            .getOffsetBuffer()
            .getInt(i * BaseVariableWidthVector.OFFSET_WIDTH)

          val dataLength = vc.getOffsetBuffer.getInt(
            (i + 1) * BaseVariableWidthVector.OFFSET_WIDTH
          ) - startOffset
          val result = new Array[Byte](dataLength)
          vc.getDataBuffer.getBytes(startOffset, result, 0, dataLength)
          // this is the total length of the data buffer.
          vc.getDataBuffer.readableBytes()
          StringInfo(
            startAddr = vc.getDataBuffer.memoryAddress(),
            position = startOffset,
            length = dataLength,
            value = new String(result, "utf8")
          )
        }.toList
      } finally vcv.close()
    }
  }

  "A set of Strings can be turned into an Arrow buffer, and we can just read it back" in {
    val stringBatch = makeStrings(100)
    assert(writeAndGet(stringBatch: _*).map(_.value) == stringBatch)
  }

  "We can pass a VarCharVector to the C program and get an output" in {
    import org.apache.arrow.memory.RootAllocator
    WithTestAllocator { alloc =>
        val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
    }

  }

}
