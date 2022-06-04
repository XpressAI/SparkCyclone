package com.nec.colvector

import com.nec.colvector.ArrayTConversions._
import com.nec.spark.agile.core._
import com.nec.util.FixedBitSet
import com.nec.util.PointerOps._
import scala.collection.mutable.{Seq => MSeq}
import scala.reflect.ClassTag
import org.bytedeco.javacpp._

object SeqOptTConversions {
  private[colvector] def constructValidityBuffer[T](input: Seq[Option[T]]): BytePointer = {
    val bitset = new FixedBitSet(input.size)
    input.zipWithIndex.foreach { case (v, i) =>
      bitset.set(i, v.nonEmpty)
    }
    bitset.toBytePointer
  }

  implicit class SeqOptTToBPCV[T <: AnyVal : ClassTag](input: Seq[Option[T]]) {
    private[colvector] def dataBuffer: BytePointer = {
      val klass = implicitly[ClassTag[T]].runtimeClass

      val buffer = if (klass == classOf[Int]) {
        val ptr = new IntPointer(input.size.toLong)
        input.asInstanceOf[Seq[Option[Int]]].zipWithIndex.foreach { case (v, i) =>
          ptr.put(i.toLong, v.getOrElse(0))
        }
        ptr

      } else if (klass == classOf[Long]) {
        val ptr = new LongPointer(input.size.toLong)
        input.asInstanceOf[Seq[Option[Long]]].zipWithIndex.foreach { case (v, i) =>
          ptr.put(i.toLong, v.getOrElse(0L))
        }
        ptr

      } else if (klass == classOf[Float]) {
        val ptr = new FloatPointer(input.size.toLong)
        input.asInstanceOf[Seq[Option[Float]]].zipWithIndex.foreach { case (v, i) =>
          ptr.put(i.toLong, v.getOrElse(0F))
        }
        ptr

      } else if (klass == classOf[Double]) {
        val ptr = new DoublePointer(input.size.toLong)
        input.asInstanceOf[Seq[Option[Double]]].zipWithIndex.foreach { case (v, i) =>
          ptr.put(i.toLong, v.getOrElse(0D))
        }
        ptr

      } else if (klass == classOf[Short]) {
        val ptr = new IntPointer(input.size.toLong)
        input.asInstanceOf[Seq[Option[Short]]].zipWithIndex.foreach { case (v, i) =>
          ptr.put(i.toLong, v.getOrElse(0: Short).toInt)
        }
        ptr

      } else {
        throw new NotImplementedError(s"Primitive type not supported: ${klass}")
      }

      buffer.asBytePointer
    }

    def toBytePointerColVector(name: String)(implicit source: VeColVectorSource): BytePointerColVector = {
      BytePointerColVector(
        source,
        name,
        VeScalarType.fromJvmType[T],
        input.size,
        Seq(
          dataBuffer,
          constructValidityBuffer(input)
        )
      )
    }
  }

  implicit class SeqOptStringToBPCV(input: Seq[Option[String]]) {
    private[colvector] def constructBuffers: (BytePointer, BytePointer, BytePointer) = {
      // Convert to UTF-32LE Array[Byte]'s
      val bytesAA = input.map { x =>
        x.map(_.getBytes("UTF-32LE")).getOrElse(Array[Byte]())
      }.toArray

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
          constructValidityBuffer(input)
        )
      )
    }
  }

  implicit class BPCVToSeqOptT(input: BytePointerColVector) {
    private[colvector] lazy val numItems = input.numItems
    private[colvector] lazy val veType = input.veType
    private[colvector] lazy val buffers = input.buffers

    private[colvector] def toStringArray: Seq[Option[String]] = {
      val dataBuffer = buffers(0)
      val startsBuffer = new IntPointer(buffers(1))
      val lensBuffer = new IntPointer(buffers(2))
      val bitset = FixedBitSet.from(buffers(3))

      val output = MSeq.fill[Option[String]](numItems)(None)
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
          output(i) = Some(new String(bytes, "UTF-32LE"))
        }
      }

      output
    }

    def toSeqOpt[T: ClassTag]: Seq[Option[T]] = {
      val klass = implicitly[ClassTag[T]].runtimeClass
      require(klass == veType.scalaType, s"Requested type ${klass.getName} does not match the VeType: ${veType}")

      val dataBuffer = buffers(0)
      val bitset = FixedBitSet.from(buffers(1))

      if (klass == classOf[Int]) {
        val buffer = new IntPointer(dataBuffer)
        val output = MSeq.fill[Option[Int]](numItems)(None)
        0.until(numItems).foreach { i =>
          if (bitset.get(i)) output(i) = Some(buffer.get(i.toLong))
        }
        output.asInstanceOf[Seq[Option[T]]]

      } else if (klass == classOf[Long]) {
        val buffer = new LongPointer(dataBuffer)
        val output = MSeq.fill[Option[Long]](numItems)(None)
        0.until(numItems).foreach { i =>
          if (bitset.get(i)) output(i) = Some(buffer.get(i.toLong))
        }
        output.asInstanceOf[Seq[Option[T]]]

      } else if (klass == classOf[Float]) {
        val buffer = new FloatPointer(dataBuffer)
        val output = MSeq.fill[Option[Float]](numItems)(None)
        0.until(numItems).foreach { i =>
          if (bitset.get(i)) output(i) = Some(buffer.get(i.toLong))
        }
        output.asInstanceOf[Seq[Option[T]]]

      } else if (klass == classOf[Double]) {
        val buffer = new DoublePointer(dataBuffer)
        val output = MSeq.fill[Option[Double]](numItems)(None)
        0.until(numItems).foreach { i =>
          if (bitset.get(i)) output(i) = Some(buffer.get(i.toLong))
        }
        output.asInstanceOf[Seq[Option[T]]]

      } else if (klass == classOf[Short]) {
        val buffer = new IntPointer(dataBuffer)
        val output = MSeq.fill[Option[Short]](numItems)(None)
        0.until(numItems).foreach { i =>
          if (bitset.get(i)) output(i) = Some(buffer.get(i.toLong).toShort)
        }
        output.asInstanceOf[Seq[Option[T]]]

      } else if (klass == classOf[String]) {
        toStringArray.asInstanceOf[Seq[Option[T]]]

      } else {
        throw new NotImplementedError(s"Conversion of BytePointerColVector to Seq[Option[${klass.getName}]] not supported")
      }
    }

    def toSeqOptAny: Seq[Option[Any]] = {
      toSeqOpt(ClassTag(input.veType.scalaType)).asInstanceOf[Seq[Option[Any]]]
    }

    def toSeqOptAny2: Seq[Option[Any]] = {
      import org.apache.spark.unsafe.types.UTF8String

      val tmp = toSeqOpt(ClassTag(input.veType.scalaType))

      val tmp2 = if (input.veType.scalaType == classOf[String]) {
        tmp.map(_.map(UTF8String.fromString))
      } else {
        tmp
      }

      tmp2.asInstanceOf[Seq[Option[Any]]]
    }
  }
}
