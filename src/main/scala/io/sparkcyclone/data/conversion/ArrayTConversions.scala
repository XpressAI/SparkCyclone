package io.sparkcyclone.data.conversion

import io.sparkcyclone.spark.agile.core._
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.vector.BytePointerColVector
import io.sparkcyclone.util.FixedBitSet
import io.sparkcyclone.util.PointerOps._
import scala.reflect.ClassTag
import org.bytedeco.javacpp._

object ArrayTConversions {
  implicit class ArrayTToBPCV[T : ClassTag](input: Array[T]) {
    private[conversion] def dataBuffer: BytePointer = {
      val klass = implicitly[ClassTag[T]].runtimeClass

      val buffer = if (klass == classOf[Int]) {
        val ptr = new IntPointer(input.size.toLong)
        ptr.put(input.asInstanceOf[Array[Int]], 0, input.size)

      } else if (klass == classOf[Long]) {
        val ptr = new LongPointer(input.size.toLong)
        ptr.put(input.asInstanceOf[Array[Long]], 0, input.size)

      } else if (klass == classOf[Float]) {
        val ptr = new FloatPointer(input.size.toLong)
        ptr.put(input.asInstanceOf[Array[Float]], 0, input.size)

      } else if (klass == classOf[Double]) {
        val ptr = new DoublePointer(input.size.toLong)
        ptr.put(input.asInstanceOf[Array[Double]], 0, input.size)

      } else if (klass == classOf[Short]) {
        val ptr = new IntPointer(input.size.toLong)
        input.asInstanceOf[Array[Short]].zipWithIndex.foreach { case (v, i) =>
          ptr.put(i.toLong, v.toInt)
        }
        ptr

      } else {
        throw new NotImplementedError(s"Primitive type not supported: ${klass}")
      }

      buffer.asBytePointer
    }

    private[conversion] def validityBuffer: BytePointer = {
      val bitset = new FixedBitSet(input.size)
      input.zipWithIndex.foreach { case (x, i) =>
        bitset.set(i, x != null)
      }

      bitset.toBytePointer
    }

    def toBytePointerColVector(name: String)(implicit source: VeColVectorSource): BytePointerColVector = {
      BytePointerColVector(
        source,
        name,
        VeScalarType.fromJvmType[T],
        input.size,
        Seq(
          dataBuffer,
          validityBuffer
        )
      )
    }
  }

  private[conversion] implicit class ExtendedArrayArrayByte(bytesAA: Array[Array[Byte]]) {
    def constructBuffers: (BytePointer, BytePointer, BytePointer) = {
      // Allocate the buffers
      val dataBuffer = new BytePointer(bytesAA.foldLeft(0)(_ + _.size).toLong)
      val startsBuffer = new IntPointer(bytesAA.size.toLong)
      val lensBuffer = new IntPointer(bytesAA.size.toLong)

      // Populate the buffers
      var pos = 0
      bytesAA.zipWithIndex.foreach { case (bytes, i) =>
        // Write the starts and lens as int32_t offsets
        startsBuffer.put(i.toLong, pos / 4)
        lensBuffer.put(i.toLong, bytes.size / 4)

        bytes.foreach { b =>
          // Copy byte over - note that can't use bulk `put()` because it always assumes position 0 in the destination buffer
          dataBuffer.put(pos.toLong, b)
          pos += 1
        }
      }

      (
        dataBuffer,
        startsBuffer.asBytePointer,
        lensBuffer.asBytePointer
      )
    }
  }

  implicit class ArrayStringToBPCV(input: Array[String]) {
    private[conversion] def validityBuffer: BytePointer = {
      val bitset = new FixedBitSet(input.size)
      input.zipWithIndex.foreach { case (x, i) =>
        bitset.set(i, x != null)
      }

      bitset.toBytePointer
    }

    private[conversion] def constructBuffers: (BytePointer, BytePointer, BytePointer) = {
      // Convert to UTF-32LE Array[Byte]'s
      val bytesAA = input.map { x =>
        if (x == null) {
          Array[Byte]()
        } else {
          x.getBytes("UTF-32LE")
        }
      }

      bytesAA.constructBuffers
    }

    def toBytePointerColVector(name: String)(implicit source: VeColVectorSource): BytePointerColVector = {
      val (dataBuffer, startsBuffer, lensBuffer) = constructBuffers

      BytePointerColVector(
        source,
        name,
        VeString,
        input.size,
        Seq(
          dataBuffer,
          startsBuffer,
          lensBuffer,
          validityBuffer
        )
      )
    }
  }

  implicit class BPCVToArrayT(input: BytePointerColVector) {
    private[conversion] lazy val numItems = input.numItems
    private[conversion] lazy val veType = input.veType
    private[conversion] lazy val buffers = input.buffers

    private[conversion] def toStringArray: Array[String] = {
      val dataBuffer = buffers(0)
      val startsBuffer = new IntPointer(buffers(1))
      val lensBuffer = new IntPointer(buffers(2))
      val bitset = FixedBitSet.from(buffers(3))

      val output = new Array[String](numItems)
      for (i <- 0 until numItems) {
        // Get the validity bit at position i
        if (bitset.get(i)) {
          // Read starts and lens as byte offsets (they are stored in BytePointerColVector as int32_t offsets)
          val start = startsBuffer.get(i) * 4
          val len = lensBuffer.get(i) * 4

          // Allocate the Array[Byte]
          val bytes = new Array[Byte](len)

          // Copy over the bytes
          dataBuffer.position(start.toLong)
          dataBuffer.get(bytes)

          // Create the String with the encoding
          output(i) = new String(bytes, "UTF-32LE")
        }
      }

      output
    }

    def toArray[T: ClassTag]: Array[T] = {
      val klass = implicitly[ClassTag[T]].runtimeClass
      require(klass == veType.scalaType, s"Requested type ${klass.getName} does not match the VeType: ${veType}")

      val dataBuffer = buffers(0)

      if (klass == classOf[Int]) {
        val output = new Array[Int](numItems)
        new IntPointer(dataBuffer).get(output)
        output.asInstanceOf[Array[T]]

      } else if (klass == classOf[Long]) {
        val output = new Array[Long](numItems)
        new LongPointer(dataBuffer).get(output)
        output.asInstanceOf[Array[T]]

      } else if (klass == classOf[Float]) {
        val output = new Array[Float](numItems)
        new FloatPointer(dataBuffer).get(output)
        output.asInstanceOf[Array[T]]

      } else if (klass == classOf[Double]) {
        val output = new Array[Double](numItems)
        new DoublePointer(dataBuffer).get(output)
        output.asInstanceOf[Array[T]]

      } else if (klass == classOf[Short]) {
        val output = new Array[Short](numItems)
        val buf = new IntPointer(dataBuffer)
        for (i <- 0 until numItems) {
          output(i) = buf.get(i.toLong).toShort
        }
        output.asInstanceOf[Array[T]]

      } else if (klass == classOf[String]) {
        toStringArray.asInstanceOf[Array[T]]

      } else {
        throw new NotImplementedError(s"Conversion of BytePointerColVector to Array[${klass.getName}] not supported")
      }
    }
  }
}
