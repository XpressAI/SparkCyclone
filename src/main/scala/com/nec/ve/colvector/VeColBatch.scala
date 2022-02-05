package com.nec.ve.colvector

import com.nec.arrow.colvector.{GenericColBatch, UnitColVector}
import com.nec.spark.agile.CFunctionGeneration.VeType
import com.nec.ve
import com.nec.ve.{VeProcess, VeSerializer}
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream,
  InputStream,
  OutputStream
}
import java.util
import scala.util.Try

//noinspection AccessorLikeMethodIsEmptyParen
final case class VeColBatch(underlying: GenericColBatch[VeColVector]) {

  def serializeToBytes()(implicit veProcess: VeProcess): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    writeToStream(new DataOutputStream(baos))
    baos.flush()
    try baos.toByteArray
    finally baos.close()
  }
  def writeToStream(out: DataOutputStream)(implicit veProcess: VeProcess): Unit = {
    import VeColBatch._
    out.writeInt(ColLengthsId)
    out.writeInt(cols.length)
    cols.foreach { colVector =>
      val descByteForm: Array[Byte] = colVector.underlying.toUnit.byteForm
      out.writeInt(DescLengthId)
      out.writeInt(descByteForm.length)
      out.writeInt(DescDataId)
      out.write(descByteForm)
      val payloadBytes = colVector.serialize()
      out.writeInt(PayloadBytesLengthId)
      out.writeInt(payloadBytes.length)
      out.writeInt(PayloadBytesId)
      out.write(payloadBytes)
    }
  }

  def nonEmpty: Boolean = underlying.nonEmpty

  def numRows = underlying.numRows
  def cols = underlying.cols

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

  val ColLengthsId = 199
  val DescLengthId = 198
  val DescDataId = 197
  val PayloadBytesLengthId = 196
  val PayloadBytesId = 195

  def ensureId(v: Int, e: Int): Unit = {
    require(v == e, s"Expected id ${e}, got ${v}")
  }

  def readFromBytes(
    bytes: Array[Byte]
  )(implicit veProcess: VeProcess, veColVectorSource: VeColVectorSource): VeColBatch = {
    val bais = new ByteArrayInputStream(bytes)
    try readFromStream(new DataInputStream(bais))
    finally bais.close()
  }
  def readFromStream(
    in: DataInputStream
  )(implicit veProcess: VeProcess, source: VeColVectorSource): VeColBatch = {
    ensureId(in.readInt(), ColLengthsId)

    val numCols = in.readInt()
    val cols = (0 until numCols).map { i =>
      try {
        ensureId(in.readInt(), DescLengthId)

        val descLength = in.readInt()
        ensureId(in.readInt(), DescDataId)
        require(descLength > 0, s"Expecting col description to be >0, is ${descLength}")
        val arr = Array.fill[Byte](descLength)(-1)
        in.read(arr)
        val unitColVector = UnitColVector.fromBytes(arr)
        ensureId(in.readInt(), PayloadBytesLengthId)
        val payloadLength = in.readInt()

        val arrPayload = Array.fill[Byte](payloadLength)(-1)
        ensureId(in.readInt(), PayloadBytesId)

        in.read(arrPayload)
        import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._
        unitColVector.deserialize(arrPayload)
      } catch {
        case e: Throwable =>
          val stuffAfter =
            (0 until 12).map(_ => Try(in.read()).toOption.fold("-")(_.toString)).toList
          throw new RuntimeException(
            s"Failed to read: stream is ${in}; there were ${numCols} columns described; we are at the ${i}th; error ${e}; bytes after = ${stuffAfter}",
            e
          )
      }
    }

    VeColBatch.fromList(cols.toList)
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
    originalCallingContext: OriginalCallingContext
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

}
