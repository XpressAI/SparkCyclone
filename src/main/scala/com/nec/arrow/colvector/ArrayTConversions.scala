package com.nec.arrow.colvector

import com.nec.spark.agile.core._
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import scala.reflect.ClassTag
import com.nec.util.FixedBitSet
import org.bytedeco.javacpp._

object ArrayTConversions {
  implicit class PrimitiveArrayToBPCV[T <: AnyVal : ClassTag](input: Array[T]) {
    private[colvector] def constructValidityBuffer(n: Int): BytePointer = {
      val size = (n / 8.0).ceil.toLong
      val buffer = new BytePointer(size)
      for (i <- 0 until size.toInt) {
        buffer.put(i, -1.toByte)
      }
      buffer
    }

    private[colvector] def dataBuffer: BytePointer = {
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

        /*
          Cast to BytePointer and manually set the capacity value to account for
          the size difference between the two pointer types (casting JavaCPP
          pointers literally copies the capacity value over as is).
        */
      new BytePointer(buffer).capacity(input.size.toLong * veScalarType.cSize)
    }

    private[colvector] def veScalarType: VeScalarType = {
      val klass = implicitly[ClassTag[T]].runtimeClass

      if (klass == classOf[Int]) {
        VeNullableInt

      } else if (klass == classOf[Long]) {
        VeNullableLong

      } else if (klass == classOf[Float]) {
        VeNullableFloat

      } else if (klass == classOf[Double]) {
        VeNullableDouble

      } else if (klass == classOf[Short]) {
        VeNullableShort

      } else {
        throw new NotImplementedError(s"No corresponding VeScalarType for primitive type: ${klass}")
      }
    }

    def toBytePointerColVector(name: String)(implicit source: VeColVectorSource): BytePointerColVector = {
      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = input.size,
          name = name,
          veType = veScalarType,
          container = None,
          buffers = List(
            Option(dataBuffer),
            // Since T <: AnyVal, they are non-nullable
            Option(constructValidityBuffer(input.size))
          ),
          variableSize = None
        )
      )
    }
  }

  private[colvector] implicit class ExtendedByteArrayArray(bytesAA: Array[Array[Byte]]) {
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
        /*
          Cast to BytePointer and manually set the capacity value to account for
          the size difference between the two pointer types (casting JavaCPP
          pointers literally copies the capacity value over as is).
        */
        new BytePointer(startsBuffer).capacity(bytesAA.size.toLong * 4),
        new BytePointer(lensBuffer).capacity(bytesAA.size.toLong * 4),
      )
    }
  }

  implicit class StringArrayToBPCV(input: Array[String]) {
    private[colvector] def validityBuffer: BytePointer = {
      val bitset = new FixedBitSet(input.size)
      input.zipWithIndex.foreach { case (x, i) =>
        bitset.set(i, x != null)
      }

      bitset.toBytePointer
    }

    private[colvector] def constructBuffers: (BytePointer, BytePointer, BytePointer) = {
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
        GenericColVector(
          source = source,
          numItems = input.size,
          name = name,
          veType = VeString,
          container = None,
          buffers = List(
            Option(dataBuffer),
            Option(startsBuffer),
            Option(lensBuffer),
            Option(validityBuffer)
          ),
          variableSize = Some(dataBuffer.limit().toInt / 4)
        )
      )
    }
  }

  implicit class BPCVToArrayT(input: BytePointerColVector) {
    private[colvector] lazy val underlying = input.underlying
    private[colvector] lazy val numItems = underlying.numItems
    private[colvector] lazy val veType = underlying.veType
    private[colvector] lazy val buffers = underlying.buffers.flatten

    private[colvector] def toStringArray: Array[String] = {
      val dataBuffer = buffers(0)
      val startsBuffer = new IntPointer(buffers(1))
      val lensBuffer = new IntPointer(buffers(2))
      val validityBuffer = buffers(3)

      val output = new Array[String](numItems)
      for (i <- 0 until numItems) {
        // Get the validity bit at psition i
        val isValid = (validityBuffer.get(i / 8) & (1 << (i % 8))) > 0

        if (isValid) {
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

      val dataBuffer = input.underlying.buffers(0).get

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
