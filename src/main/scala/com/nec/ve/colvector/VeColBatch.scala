package com.nec.ve.colvector

import com.nec.arrow.colvector.{GenericColBatch, UnitColBatch, UnitColVector}
import com.nec.spark.agile.core.VeType
import com.nec.ve
import com.nec.ve.{VeProcess, VeProcessMetrics}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import java.io._
import scala.util.Try

//noinspection AccessorLikeMethodIsEmptyParen
final case class VeColBatch(underlying: GenericColBatch[VeColVector]) {
  def serializeToStreamSize: Int = {
    List(4, 4) ++ cols.flatMap { col =>
      List(4, 4, 4, col.underlying.toUnit.streamedSize, 4, 4, 4, col.serializedSize)
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
      colVector.underlying.toUnit.toStreamFast(dataOutputStream)
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
    val vecs = underlying.cols.map(_.toArrowVector())
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
        val unitColVector = UnitColVector.fromStreamFast(din)
        ensureId(din.readInt(), PayloadBytesLengthId)
        // ignored here, because we read stream-based
        val payloadLength = din.readInt()
        ensureId(din.readInt(), PayloadBytesId)
        unitColVector.deserializeFromStream(din)
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

  type VeColVector = com.nec.ve.colvector.VeColVector
  val VeColVector = com.nec.ve.colvector.VeColVector

  def apply(numRows: Int, cols: List[VeColVector]): VeColBatch =
    ve.VeColBatch(GenericColBatch(numRows, cols))

  def fromList(lv: List[VeColVector]): VeColBatch = {
    assert(lv.nonEmpty)
    VeColBatch(GenericColBatch(numRows = lv.head.underlying.numItems, lv))
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
          val col = columnarBatch.column(colNo)
          VeColVector.fromVectorColumn(numRows = columnarBatch.numRows(), source = col)
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
