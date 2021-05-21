import CountArrowStringsSpec.schema
import org.apache.arrow.flatbuf.{Message, MessageHeader, RecordBatch}
import org.apache.arrow.vector.{FieldVector, VarCharVector}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.commons.codec.binary.Hex
import org.scalatest.freespec.AnyFreeSpec

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.util

object CountArrowStringsSpec {
  val schema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(
    """{"fields": [{"name": "value", "nullable" : true, "type": {"name": "utf8"}, "children": []}]}"""
  )
}

final class CountArrowStringsSpec extends AnyFreeSpec {

  def arrowStrings(strings: String*): Array[Byte] = {
    import org.apache.arrow.memory.RootAllocator
    import org.apache.arrow.vector.VectorSchemaRoot
    val alloc = new RootAllocator(Integer.MAX_VALUE)

    val baos = new ByteArrayOutputStream()

    try {
      val vcv = schema.findField("value").createVector(alloc).asInstanceOf[VarCharVector]
      vcv.allocateNew()
      try {
        strings.view.zipWithIndex.foreach { case (str, idx) =>
          vcv.setSafe(idx, str.getBytes("utf8"), 0, str.length)
        }
        vcv.setValueCount(strings.length)
        val root = new VectorSchemaRoot(schema, util.Arrays.asList(vcv: FieldVector), 2)
        root.setRowCount(strings.length)
        val arrowStreamWriter = new ArrowStreamWriter(root, null, baos)
        arrowStreamWriter.start()
        arrowStreamWriter.writeBatch()
        arrowStreamWriter.end()
        baos.toByteArray
      } finally vcv.close()
    } finally alloc.close()
  }

  "We can get a Byte Array which contains both the Strings" in {
    val string1 = ('A' to 'Z').mkString
    val string2 = "XAY"
    assert(arrowStrings(string1, string2).containsSlice(string1.getBytes("UTF-8")))
    assert(arrowStrings(string1, string2).containsSlice(string2.getBytes("UTF-8")))
  }

  def readStrings(bytes: Array[Byte]): List[String] = {
    Nil
  }

  "We can retrieve the Strings back from a raw Byte Array" in {
    info("We do it in Java so that we can then do it in C")

    assert(readStrings(arrowStrings("ABC", "DEF")) == List("ABC", "DEF"))
  }

  def paddingLocation(size: Int): Int = {
    if (size % 8 == 0) size
    else (1 + (size / 8)) * 8
  }

  type BodyOffset = Int
  // Padding is 8-bytes
  val ContinuationSize = 4
  val MetadataSizeSize = 4
  case class MessageWithBody(message: Message, body: List[Byte]) {

    def recordBatch: Option[RecordBatch] = {}

  }

  object MessageWithBody {
    def unapply(input: List[Byte]): Option[(MessageWithBody, Option[List[Byte]])] = {
      if (input.isEmpty) None
      else {
        val byteBuffer = ByteBuffer
          .wrap(input.toArray)
          .order(ByteOrder.LITTLE_ENDIAN)
        assert(byteBuffer.getInt(0) == 0xffffffff)
        val metadataSize = byteBuffer.getInt(ContinuationSize)
        if (metadataSize == 0) None
        else
          Option {
            byteBuffer.position(ContinuationSize + MetadataSizeSize)
            val message = org.apache.arrow.flatbuf.Message
              .getRootAsMessage(byteBuffer)

            val bodyOffset = paddingLocation(ContinuationSize + MetadataSizeSize + metadataSize)
            MessageWithBody(
              message,
              input.drop(bodyOffset).take(message.bodyLength().toInt)
            ) -> Option {
              input
                .drop(bodyOffset)
                .drop(paddingLocation(message.bodyLength().toInt))
            }.filter(_.nonEmpty)
          }
      }
    }
  }

  def readMessages(byteArray: Array[Byte]): Iterator[MessageWithBody] = {
    var remaining = byteArray
    Iterator
      .continually {
        val r = MessageWithBody.unapply(remaining.toList)
        r.flatMap(_._2).foreach(lb => remaining = lb.toArray)
        r.map(_._1)
      }
      .takeWhile(_.isDefined)
      .flatten
  }

  "The byte array's structure" - {
    val byteArray = arrowStrings("ABC", "DEF")
    val byteBuffer = ByteBuffer
      .wrap(byteArray)
      .order(ByteOrder.LITTLE_ENDIAN)

    "We can read out a metadata message" in {
      val MessageWithBody(msg, _) = readMessages(byteArray).next()
      assert(msg.version() == 4)
      assert(msg.headerType() == MessageHeader.Schema)
      // this is the schema -- but it's weird that body length is 0?
      assert(msg.headerType() == 1)
      assert(msg.bodyLength() == 0L)
    }

    "We can read out the second metadata message" in {
      val MessageWithBody(msg, _) = readMessages(byteArray).drop(1).next()

      assert(msg.version() == 4)
      assert(msg.headerType() == 3)
      assert(msg.headerType() == MessageHeader.RecordBatch)
      assert(msg.bodyLength() == 32L)
      byteBuffer.position(0)
    }

    "There are only 2 messages" in {
      assert(readMessages(byteArray).size == 2)
    }

    "Padding computation function works" - {
      "If input is size 2, we get 8" in {
        assert(paddingLocation(2) == 8)
      }
      "If input is size 7, we get 8" in {
        assert(paddingLocation(7) == 8)
      }
      "If input is size 8, we get 8" in {
        assert(paddingLocation(8) == 8)
      }
      "If input is size 9, we get 16" in {
        assert(paddingLocation(9) == 16)
      }
    }
    "We can get the header in binary format" ignore {
      info(Hex.encodeHexString(byteArray))
    }
  }

  /**
   * Rather than passing Arrow to VEO, we should pass buffer locations instead;
   * this way we can reuse much of the Java infrastructure without breaking the bank
   * and having to reimplement everything in C; or relying on
   * including a 600-line C++ header.
   */
}
