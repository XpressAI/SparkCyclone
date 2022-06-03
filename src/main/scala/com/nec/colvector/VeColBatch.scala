package com.nec.colvector

import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.ArrowVectorConversions._
import com.nec.colvector.SparkSqlColumnVectorConversions._
import com.nec.spark.agile.core.VeType
import com.nec.util.CallContext
import com.nec.ve.VeProcessMetrics
import com.nec.vectorengine.VeProcess
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

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

  def streamedSize: Int = {
    Seq(4, 4) ++ columns.flatMap { col =>
      Seq(4, 4, 4, col.toUnitColVector.streamedSize, 4, 4, 4, col.bufferSizes.sum)
    }
  }.sum

  def toStream(stream: DataOutputStream)(implicit process: VeProcess,
                                         metrics: VeProcessMetrics): Unit = {
    val hostBuffersAsync = columns.map(_.toBytePointersAsync())
    val channel = Channels.newChannel(stream)

    stream.writeInt(VeColBatch.ColLengthsId)
    stream.writeInt(columns.size)
    columns.zip(hostBuffersAsync).foreach { case (vec, buffers) =>
      stream.writeInt(VeColBatch.DescLengthId)
      stream.writeInt(-1)
      stream.writeInt(VeColBatch.DescDataId)
      vec.toUnitColVector.toStream(stream)
      stream.writeInt(VeColBatch.PayloadBytesLengthId)
      // no bytes length as it's a stream here
      stream.writeInt(-1)
      stream.writeInt(VeColBatch.PayloadBytesId)
      buffers.map(_.get).filterNot(_.limit() == 0).foreach{ bytePointer =>
        val numWritten = channel.write(bytePointer.asBuffer())
        require(numWritten == bytePointer.limit(), s"Written ${numWritten}, expected ${bytePointer.limit()}")
      }
    }
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

  def fromStream(din: DataInputStream)(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    context: CallContext
  ): VeColBatch = {
    ensureId(din.readInt(), ColLengthsId)

    val numCols = din.readInt()
    val columns = (0 until numCols).map { i =>
      try {
        ensureId(din.readInt(), DescLengthId)

        // not used, stream based now
        val descLength = din.readInt()
        ensureId(din.readInt(), DescDataId)
        val unitColVector = UnitColVector.fromStream(din)
        ensureId(din.readInt(), PayloadBytesLengthId)
        // ignored here, because we read stream-based
        val payloadLength = din.readInt()
        ensureId(din.readInt(), PayloadBytesId)
        unitColVector.withData(din)
      } catch {
        case e: Throwable =>
          val stuffAfter =
            (0 until 12).map(_ => Try(din.read()).toOption.fold("-")(_.toString)).toList
          throw new RuntimeException(
            s"Failed to read: stream is ${din}; there were ${numCols} columns described; we are at the ${i}th; error ${e}; bytes after = ${stuffAfter}",
            e
          )
      }
    }.map(_.apply()).map(_.get)

    VeColBatch(columns)
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
