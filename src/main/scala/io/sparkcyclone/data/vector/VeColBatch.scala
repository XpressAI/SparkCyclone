package io.sparkcyclone.data.vector

import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.transfer.{BpcvTransferDescriptor, TransferDescriptor, UcvTransferDescriptor}
import io.sparkcyclone.data.conversion.ArrayTConversions._
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions._
import io.sparkcyclone.spark.codegen.core.VeType
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.vectorengine.{VeProcess, VectorEngine}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.bytedeco.javacpp.BytePointer
import java.io._
import java.nio.channels.Channels
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

final case class VeColBatch(columns: Seq[VeColVector]) {
  def toCPUSeq[T: TypeTag](implicit process: VeProcess): Seq[T] = {
    val tag = implicitly[TypeTag[T]]
    tag.tpe.asInstanceOf[TypeRef].args match {
      case Nil =>
        toArray[T](0)(ClassTag(tag.mirror.runtimeClass(tag.tpe)), process).toSeq

      case args =>
        args.zipWithIndex.map { case (t, idx) =>
          toArray(idx)(ClassTag(tag.mirror.runtimeClass(t)), process).toSeq
        }.transpose.map { r =>
          val size = r.size
          if(r.isEmpty || size > 22){
            throw new IllegalArgumentException(s"Can not create tuple with size ${size}")
          }
          size match {
            case 1 => Tuple1(r.head)
            case 2 => (r(0), r(1))
            case 3 => (r(0), r(1), r(2))
            case 4 => (r(0), r(1), r(2), r(3))
            case 5 => (r(0), r(1), r(2), r(3), r(4))
            case 6 => (r(0), r(1), r(2), r(3), r(4), r(5))
            case 7 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6))
            case 8 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7))
            case 9 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8))
            case 10 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9))
            case 11 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10))
            case 12 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11))
            case 13 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12))
            case 14 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13))
            case 15 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14))
            case 16 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15))
            case 17 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16))
            case 18 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17))
            case 19 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18))
            case 20 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18), r(19))
            case 21 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18), r(19), r(20))
            case 22 => (r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18), r(19), r(20), r(21))
          }
        }.asInstanceOf[Seq[T]]
    }
  }

  def toArray[T: ClassTag](idx: Int)(implicit process: VeProcess): Array[T] = {
    columns(idx).toBytePointerColVector.toArray[T]
  }

  def toBytePointerColBatch(implicit process: VeProcess): BytePointerColBatch = {
    BytePointerColBatch(columns.map(_.toBytePointerColVector))
  }

  def streamedSize: Int = {
    val ucolumns = columns.map(_.toUnitColVector)

    Seq(4, 4) ++
      ucolumns.flatMap(c => Seq(4, c.streamedSize)) ++
      Seq(4, 8) ++
      Seq(TransferDescriptor.bufferSize(ucolumns).toInt)
  }.sum

  def toStream(stream: DataOutputStream)(implicit process: VeProcess): Unit = {
    val bcolumns = columns.map(_.toBytePointerColVector)
    val channel = Channels.newChannel(stream)

    // Set number of columns
    stream.writeInt(VeColBatch.ColLengthsId)
    stream.writeInt(bcolumns.size)

    // Write UnitColVector data for each column
    bcolumns.foreach { column =>
      stream.writeInt(VeColBatch.DescDataId)
      column.toUnitColVector.toStream(stream)
    }

    // Create the transfer descriptor
    val descriptor = BpcvTransferDescriptor(Seq(bcolumns))

    // Generate the buffer and write its size
    stream.writeInt(VeColBatch.PayloadBytesLengthId)
    stream.writeLong(descriptor.buffer.limit())

    // Write the buffer to the stream via a channel
    channel.write(descriptor.buffer.asBuffer)

    // Close the descriptor
    descriptor.close
  }

  def toBytes(implicit process: VeProcess, metrics: VeProcessMetrics): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val writer = new ObjectOutputStream(stream)
    writer.writeObject(toUnit)
    writer.writeInt(columns.size)
    columns.foreach { vec => writer.writeObject(vec.toBytes) }
    writer.flush
    writer.close
    stream.flush
    stream.toByteArray
  }

  def veTypes: Seq[VeType] = {
    columns.map(_.veType)
  }

  def nonEmpty: Boolean = {
    numRows > 0
  }

  def numRows: Int = {
    columns.headOption.map(_.numItems).getOrElse(0)
  }

  def toUnit: UnitColBatch = {
    UnitColBatch(columns.map(_.toUnitColVector))
  }

  def free()(implicit source: VeColVectorSource,
             process: VeProcess,
             context: CallContext): Unit = {
    process.freeSeq(columns.flatMap(_.closeAndReturnAllocations))
  }

  def toArrowColumnarBatch(implicit allocator: BufferAllocator,
                           process: VeProcess): ColumnarBatch = {
    val vecs = columns.map(_.toBytePointerColVector.toArrowVector)
    val cb = new ColumnarBatch(vecs.map(col => new ArrowColumnVector(col)).toArray)
    cb.setNumRows(numRows)
    cb
  }

  def toSparkColumnarBatch: ColumnarBatch = {
    val vecs = columns.map(_.toSparkColumnVector)
    val cb = new ColumnarBatch(vecs.toArray)
    cb.setNumRows(numRows)
    cb
  }

  def totalBufferSize: Int = {
    columns.flatMap(_.bufferSizes).foldLeft(0)(_ + _)
  }
}

object VeColBatch {
  final val ColLengthsId = 193
  final val DescLengthId = 198
  final val DescDataId = 197
  final val PayloadBytesLengthId = 196
  final val PayloadBytesId = 195

  def ensureId(v: Int, e: Int): Unit = {
    require(v == e, s"Expected id ${e}, got ${v}")
  }

  def fromStream(stream: DataInputStream)(implicit engine: VectorEngine,
                                          context: CallContext): VeColBatch = {
    val channel = Channels.newChannel(stream)

    // Read the number of columns
    ensureId(stream.readInt, ColLengthsId)
    val ncolumns = stream.readInt

    // Read the UnitColVector data for each column
    val ucolumns = 0.until(ncolumns).map { _ =>
      ensureId(stream.readInt, DescDataId)
      UnitColVector.fromStream(stream)
    }

    // Read the transfer descriptor buffer size
    ensureId(stream.readInt, PayloadBytesLengthId)
    val nbytes = stream.readLong

    // Read the buffer
    val buffer = {
      val ptr = new BytePointer(nbytes)
      val buf = ptr.asBuffer
      var bytesRead = 0L
      while (bytesRead < nbytes) {
        bytesRead += channel.read(buf)
      }
      ptr
    }

    // Construct the UcvTransferDescriptor from the UnitColVector's and buffer
    val descriptor = UcvTransferDescriptor(ucolumns, buffer)

    // Perform the transfer and return VeColBatch
    engine.executeTransfer(descriptor)
  }

  def fromBytes(data: Array[Byte])(implicit source: VeColVectorSource,
                                   process: VeProcess,
                                   context: CallContext,
                                   metrics: VeProcessMetrics): VeColBatch = {
    val stream = new ByteArrayInputStream(data)
    val reader = new ObjectInputStream(stream)
    val batch = reader.readObject.asInstanceOf[UnitColBatch]
    val numCols = reader.readInt
    val arrays = (0 until numCols).map { _ =>
      reader.readObject.asInstanceOf[Array[Byte]]
    }
    batch.withData(arrays)
  }

  def fromArrowColumnarBatch(batch: ColumnarBatch)(implicit source: VeColVectorSource,
                                                   process: VeProcess,
                                                   context: CallContext,
                                                   metrics: VeProcessMetrics): VeColBatch = {
    VeColBatch(
      (0 until batch.numCols).map { i =>
        batch.column(i)
          .getArrowValueVector
          .toBytePointerColVector
          .asyncToVeColVector
      }.map(_.apply()).map(_.get)
    )
  }

  def from(columns: VeColVector*): VeColBatch = {
    assert(columns.nonEmpty)
    VeColBatch(columns)
  }

  def empty: VeColBatch = {
    VeColBatch(Seq.empty)
  }
}
