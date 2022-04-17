package com.nec.colvector

import com.nec.colvector.ArrowVectorConversions._
import com.nec.colvector.SparkSqlColumnVectorConversions._
import com.nec.colvector.VeColBatch.VeColVectorSource
import com.nec.spark.agile.core.VeType
import com.nec.util.DateTimeOps.ExtendedInstant
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.{VeProcess, VeProcessMetrics}
import com.nec.{colvector, ve}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import java.io._
import java.time.Instant
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.util.Try

//noinspection AccessorLikeMethodIsEmptyParen
final case class VeColBatch(underlying: GenericColBatch[VeColVector]) {
  def toCPUSeq[T: TypeTag](): Seq[T] = {
    val tag = implicitly[TypeTag[T]]
    tag.tpe.asInstanceOf[TypeRef].args match {
      case Nil => toArray[T](0)(ClassTag(tag.mirror.runtimeClass(tag.tpe))).toSeq
      case args => args.zipWithIndex.map { case (t, idx) =>
          toArray(idx)(ClassTag(tag.mirror.runtimeClass(t))).toSeq
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

  def toArray[T: ClassTag](colIdx: Int): Array[T] = {
    import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
    implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

    val klass = implicitly[ClassTag[T]].runtimeClass

    val arrowBatch = toArrowColumnarBatch()
    (if (klass == classOf[Int]) {
      arrowBatch.column(colIdx).getInts(0, arrowBatch.numRows())
    } else if (klass == classOf[Long]) {
      arrowBatch.column(colIdx).getLongs(0, arrowBatch.numRows())
    } else if (klass == classOf[Float]) {
      arrowBatch.column(colIdx).getFloats(0, arrowBatch.numRows())
    } else if (klass == classOf[Double]) {
      arrowBatch.column(colIdx).getDoubles(0, arrowBatch.numRows())
    } else if (klass == classOf[Instant]) {
      arrowBatch.column(0).getLongs(0, arrowBatch.numRows()).map(ExtendedInstant.fromFrovedisDateTime)
    } else {
      throw new NotImplementedError(s"Cannot extract Array[T] from ColumnarBatch for T = ${klass}")
    }).asInstanceOf[Array[T]]
  }

  def serializeToStreamSize: Int = {
    List(4, 4) ++ cols.flatMap { col =>
      List(4, 4, 4, col.toUnit.streamedSize, 4, 4, 4, col.serializedSize)
    }
  }.sum

  def serializeToStream(
    dataOutputStream: DataOutputStream
  )(implicit veProcess: VeProcess, cycloneMetrics: VeProcessMetrics): Unit = {
    import VeColBatch._
    dataOutputStream.writeInt(ColLengthsId)
    dataOutputStream.writeInt(cols.length)
    cols.foreach { colVector =>
      dataOutputStream.writeInt(DescLengthId)
      dataOutputStream.writeInt(-1)
      dataOutputStream.writeInt(DescDataId)
      colVector.toUnit.toStream(dataOutputStream)
      dataOutputStream.writeInt(PayloadBytesLengthId)
      // no bytes length as it's a stream here
      dataOutputStream.writeInt(-1)
      dataOutputStream.writeInt(PayloadBytesId)
      colVector.serializeToStream(dataOutputStream)
    }

  }

  def serialize()(implicit veProcess: VeProcess, cycloneMetrics: VeProcessMetrics): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    objectOutputStream.writeObject(toUnit)
    objectOutputStream.writeInt(cols.size)
    underlying.cols.map(_.serialize()).foreach(objectOutputStream.writeObject)
    objectOutputStream.flush()
    objectOutputStream.close()
    byteArrayOutputStream.flush()
    byteArrayOutputStream.toByteArray
  }

  def nonEmpty: Boolean = underlying.nonEmpty

  def numRows: Int = underlying.numRows
  def cols: List[VeColVector] = underlying.cols

  def toUnit: UnitColBatch = UnitColBatch(underlying.map(_.toUnit))

  def free()(implicit
    veProcess: VeProcess,
    veColVectorSource: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): Unit =
    cols.foreach(_.free())

  def toArrowColumnarBatch()(implicit
    bufferAllocator: BufferAllocator,
    veProcess: VeProcess
  ): ColumnarBatch = {
    val vecs = underlying.cols.map(_.toBytePointerVector.toArrowVector)
    val cb = new ColumnarBatch(vecs.map(col => new ArrowColumnVector(col)).toArray)
    cb.setNumRows(underlying.numRows)
    cb
  }

  def toInternalColumnarBatch(): ColumnarBatch = {
    val vecs = underlying.cols.map(_.toInternalVector())
    val cb = new ColumnarBatch(vecs.toArray)
    cb.setNumRows(underlying.numRows)
    cb
  }

  def totalBufferSize: Int = underlying.cols.flatMap(_.underlying.bufferSizes).sum
}

object VeColBatch {
  val ColLengthsId = 193
  val DescLengthId = 198
  val DescDataId = 197
  val PayloadBytesLengthId = 196
  val PayloadBytesId = 195

  def ensureId(v: Int, e: Int): Unit = {
    require(v == e, s"Expected id ${e}, got ${v}")
  }

  def fromStream(din: DataInputStream)(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext
  ): VeColBatch = {
    ensureId(din.readInt(), ColLengthsId)

    val numCols = din.readInt()
    val cols = (0 until numCols).map { i =>
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
    }

    VeColBatch.fromList(cols.toList)
  }

  def deserialize(data: Array[Byte])(implicit
    veProcess: VeProcess,
    originalCallingContext: OriginalCallingContext,
    source: VeColVectorSource,
    cycloneMetrics: VeProcessMetrics
  ): VeColBatch = {
    val byteArrayInputStream = new ByteArrayInputStream(data)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)
    val unitObj = objectInputStream.readObject().asInstanceOf[UnitColBatch]
    val numCols = objectInputStream.readInt()
    val seqs = (0 until numCols).map { _ =>
      objectInputStream.readObject().asInstanceOf[Array[Byte]]
    }.toList
    unitObj.deserialize(seqs)
  }

  type VeColVector = colvector.VeColVector
  val VeColVector = colvector.VeColVector

  def apply(numRows: Int, cols: List[VeColVector]): VeColBatch =
    ve.VeColBatch(GenericColBatch(numRows, cols))

  def fromList(lv: List[VeColVector]): VeColBatch = {
    assert(lv.nonEmpty)
    VeColBatch(GenericColBatch(numRows = lv.head.underlying.numItems, lv))
  }

  def from(vecs: VeColVector*): VeColBatch = {
    assert(vecs.nonEmpty)
    VeColBatch(GenericColBatch(numRows = vecs.head.underlying.numItems, vecs.toList))
  }

  def empty: VeColBatch = {
    VeColBatch(GenericColBatch(0, List.empty))
  }
  final case class ColumnGroup(veType: VeType, relatedColumns: List[VeColVector]) {}

  final case class VeBatchOfBatches(cols: Int, rows: Int, batches: List[VeColBatch]) {
    def isEmpty: Boolean = !nonEmpty
    def nonEmpty: Boolean = rows > 0

    /** Transpose to get the columns from each batch aligned, ie [[1st col of 1st batch, 1st col of 2nd batch, ...], [2nd col of 1st batch, ...] */
    def groupedColumns: List[ColumnGroup] = {
      if (batches.isEmpty) Nil
      else {
        batches.head.underlying.cols.zipWithIndex.map { case (vcv, idx) =>
          ColumnGroup(
            veType = vcv.underlying.veType,
            relatedColumns = batches
              .map(_.underlying.cols.apply(idx))
              .ensuring(
                cond = _.forall(_.underlying.veType == vcv.underlying.veType),
                msg = "All types should match up"
              )
          )
        }
      }
    }
  }

  object VeBatchOfBatches {
    def fromVeColBatches(list: List[VeColBatch]): VeBatchOfBatches = {
      VeBatchOfBatches(
        cols = list.head.underlying.cols.size,
        rows = list.map(_.underlying.numRows).sum,
        batches = list
      )
    }
  }

  def fromArrowColumnarBatch(columnarBatch: ColumnarBatch)(implicit
    veProcess: VeProcess,
    source: VeColVectorSource,
    originalCallingContext: OriginalCallingContext,
    cycloneMetrics: VeProcessMetrics
  ): VeColBatch = {
    VeColBatch(
      GenericColBatch(
        numRows = columnarBatch.numRows(),
        cols = (0 until columnarBatch.numCols()).map { colNo =>
          val column = columnarBatch.column(colNo)
          column.getArrowValueVector.toBytePointerColVector.toVeColVector
        }.toList
      )
    )
  }

  final case class VeColVectorSource(identifier: String)

  object VeColVectorSource {

    def make(implicit fullName: sourcecode.FullName, line: sourcecode.Line): VeColVectorSource =
      VeColVectorSource(s"${fullName.value}#${line.value}")

    object Automatic {
      implicit def veColVectorSource(implicit
        fullName: sourcecode.FullName,
        line: sourcecode.Line
      ): VeColVectorSource = make
    }
  }

}
