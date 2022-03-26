package com.nec.arrow.colvector

import com.nec.arrow.colvector.TypeLink.{ArrowToVe, VeToArrow}
import com.nec.spark.agile.core._
import com.nec.util.ReflectionOps._
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import java.nio.charset.StandardCharsets
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.compare.VectorEqualsVisitor
import org.bytedeco.javacpp._
import sun.misc.Unsafe

object ArrowVectorConversions {
  implicit class ValueVectorEqualityChecks(vector: ValueVector) {
    def === (other: ValueVector): Boolean = {
      VectorEqualsVisitor.vectorEquals(vector, other)
    }
  }

  implicit class BPCVToFieldVector(input: BytePointerColVector) {
    private[colvector] lazy val underlying = input.underlying
    private[colvector] lazy val numItems = underlying.numItems
    private[colvector] lazy val buffers = underlying.buffers.flatten
    private[colvector] lazy val bufferAdresses = underlying.buffers.flatten.map(_.address())

    private[colvector] def toScalarArrow(typ: VeScalarType)(implicit allocator: BufferAllocator): FieldVector = {
      val vec = VeToArrow(typ).makeArrow(underlying.name)(allocator)
      if (numItems > 0) {
        val dataSize = numItems * typ.cSize
        vec.setValueCount(numItems)

        implicitly[Unsafe].copyMemory(
          bufferAdresses(1),
          vec.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )

        implicitly[Unsafe].copyMemory(bufferAdresses(0), vec.getDataBufferAddress, dataSize)
      }
      vec
    }

    private[colvector] def toShortArrow(implicit allocator: BufferAllocator): SmallIntVector = {
      val vec = new SmallIntVector(underlying.name, allocator)
      if (numItems > 0) {
        vec.setValueCount(numItems)

        // Cast the data buffer as int[] and read directly into SmallIntVector
        val buffer = new IntPointer(buffers(0))
        for (i <- 0 until numItems) {
          vec.set(i, buffer.get(i).toShort)
        }

        implicitly[Unsafe].copyMemory(
          bufferAdresses(1),
          vec.getValidityBufferAddress,
          Math.ceil(numItems / 64.0).toInt * 8
        )
      }
      vec
    }

    private[colvector] def toVarCharArrow(implicit allocator: BufferAllocator): VarCharVector = {
      val vec = new VarCharVector(underlying.name, allocator)

      if (numItems > 0) {
        // Allocate and set count
        vec.allocateNew(buffers(0).capacity(), numItems)
        vec.setValueCount(numItems)

        val dataBuffer = buffers(0)
        val startsBuffer = new IntPointer(buffers(1))
        val lensBuffer = new IntPointer(buffers(2))
        val validityBuffer = buffers(3)

        for (i <- 0 until numItems) {
          // Get the validity bit at psition i
          val isValid = (validityBuffer.get(i / 8) & (1 << (i % 8))) > 0

          if (isValid) {
            // Read starts and lens as byte offsets (they are stored in BytePointerColVector as int32_t offsets)
            val start = startsBuffer.get(i.toLong) * 4
            val len = lensBuffer.get(i.toLong) * 4

            // Copy the chunk of the dataBuffer
            val bytes = new Array[Byte](len)
            for (i <- 0 until len) {
              bytes(i) = dataBuffer.get(start + i)
            }

            // Parse it as UTF-32LE string and convert to UTF-8 bytes
            val utf8bytes = new String(bytes,  "UTF-32LE").getBytes(StandardCharsets.UTF_8)

            // Write to Arrow
            vec.set(i, utf8bytes)
          }
        }
      }

      vec
    }

    def toArrowVector(implicit bufferAllocator: BufferAllocator): FieldVector = {
      underlying.veType match {
        case VeNullableShort =>
          // Specialize this case because Int values in VeNullableShort need to be cast to Short
          toShortArrow

        case typ: VeScalarType if VeToArrow.contains(typ) =>
          toScalarArrow(typ)

        case VeString =>
          toVarCharArrow

        case other =>
          throw new NotImplementedError(s"Conversion of BytePointerColVector of VeType ${other} to Arrow not supported")
      }
    }
  }

  implicit class ValueVectorToToBPCV(vector: ValueVector) {
    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      vector match {
        case vec: SmallIntVector =>
          // Specialize this case because values need to be cast to Int first
          vec.toBytePointerColVector

        case vec: BaseFixedWidthVector if ArrowToVe.contains(vec.getClass) =>
          vec.toBytePointerColVector

        case vec: VarCharVector =>
          vec.toBytePointerColVector

        case other =>
          throw new NotImplementedError(s"Conversion of ${other.getClass.getName} to BytePointerColVector not supported")
      }
    }
  }

  implicit class BaseFixedWidthVectorToBPCV(vector: BaseFixedWidthVector) {
    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      val veType = ArrowToVe.get(vector.getClass) match {
        case Some(link) => link.veScalarType
        case None => throw new IllegalArgumentException(s"No corresponding VeType found for Arrow type '${vector.getClass.getName}'")
      }

      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = vector.getValueCount,
          name = vector.getName,
          veType = veType,
          container = None,
          buffers = List(
            Option(new BytePointer(vector.getDataBuffer.nioBuffer)),
            Option(new BytePointer(vector.getValidityBuffer.nioBuffer))
          ),
          variableSize = None
        )
      )
    }
  }

  implicit class SmallIntVectorToBPCV(vector: SmallIntVector) {
    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      // Convert the values to Ints
      val buffer = new IntPointer(vector.getValueCount.toLong)
      for (i <- 0 until vector.getValueCount) {
        val x = if (vector.isNull(i)) 0 else vector.get(i).toInt
        buffer.put(i.toLong, x)
      }

      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = vector.getValueCount,
          name = vector.getName,
          // Keep the VE type information here
          veType = VeNullableShort,
          container = None,
          buffers = List(
            /*
              Cast to BytePointer and manually set the capacity value to account
              for the size difference between the two pointer types (casting
              JavaCPP pointers literally copies the capacity value over as is).
            */
            Option(new BytePointer(buffer).capacity(vector.getValueCount.toLong * 4)),
            Option(new BytePointer(vector.getValidityBuffer.nioBuffer))
          ),
          variableSize = None
        )
      )
    }
  }

  implicit class VarCharVectorToBPCV(vector: VarCharVector) {
    private[colvector] def constructBuffers: (BytePointer, BytePointer, BytePointer) = {
      /*
        Compute the buffer length

        NOTE: We compute this manually because `vector.getDataBuffer.readableBytes`
        appears to return 0 unconditionally.
      */
      val bufferlen = {
        var len = 0
        for (i <- 0 until vector.getValueCount) {
          if (! vector.isNull(i)) {
            len += vector.getObject(i).toString.getBytes("UTF-32LE").size
          }
        }
        len
      }

      val dataBuffer = new BytePointer(bufferlen)
      val startsBuffer = new IntPointer(vector.getValueCount.toLong)
      val lensBuffer = new IntPointer(vector.getValueCount.toLong)

      var pos = 0
      for (i <- 0 until vector.getValueCount) {
        if (! vector.isNull(i)) {
          // Get the string as bytes
          val bytes = vector.getObject(i).toString.getBytes("UTF-32LE")

          // Write the starts and lens as int32_t offsets
          startsBuffer.put(i.toLong, pos / 4)
          lensBuffer.put(i.toLong, bytes.size / 4)

          // Write to BytePointer
          bytes.foreach { b =>
            // Copy byte over - note that can't use bulk `put()` because it always assumes position 0 in the destination buffer
            dataBuffer.put(pos.toLong, b)
            pos += 1
          }
        }
      }

      (
        dataBuffer,
        /*
          Cast to BytePointer and manually set the capacity value to account for
          the size difference between the two pointer types (casting JavaCPP
          pointers literally copies the capacity value over as is).
        */
        new BytePointer(startsBuffer).capacity(vector.getValueCount.toLong * 4),
        new BytePointer(lensBuffer).capacity(vector.getValueCount.toLong * 4)
      )
    }

    def toBytePointerColVector(implicit source: VeColVectorSource): BytePointerColVector = {
      val (dataBuffer, startsBuffer, lensBuffer) = constructBuffers
      BytePointerColVector(
        GenericColVector(
          source = source,
          numItems = vector.getValueCount,
          name = vector.getName,
          veType = VeString,
          container = None,
          buffers = List(
            Option(dataBuffer),
            Option(startsBuffer),
            Option(lensBuffer),
            Option(new BytePointer(vector.getValidityBuffer.nioBuffer))
          ),
          variableSize = Some((dataBuffer.limit() / 4).toInt)
        )
      )
    }
  }
}
