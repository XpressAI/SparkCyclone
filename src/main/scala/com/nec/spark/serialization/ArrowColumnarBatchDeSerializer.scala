package com.nec.spark.serialization

import com.nec.spark.planning.CEvaluationPlan.HasFieldVector.RichColumnVector
import com.nec.util.ReflectionOps._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{FieldVector, Float8Vector, VectorSchemaRoot}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

object ArrowColumnarBatchDeSerializer extends Serializable {

  implicit class RichFieldVector(subject: FieldVector) {
    def append(other: FieldVector): Unit = {
      val initialValueCount = subject.getValueCount
      val newValueCount = initialValueCount + other.getValueCount
      subject.setValueCount(newValueCount)
      (0 until other.getValueCount).foreach { otherI =>
        subject.copyFromSafe(otherI, initialValueCount + otherI, other)
      }
    }
  }

  def deserializeIterator(
    iterator: Iterator[Array[Byte]]
  )(implicit bufferAllocator: BufferAllocator): Option[ColBatchWithReader] = {
    if (!iterator.hasNext) {
      Option.empty
    } else {
      val first = iterator.next()
      val initial = deserialize(first)

      while (iterator.hasNext) {
        val other = iterator.next()
        val otherAr = deserialize(other)
        if (otherAr.columnarBatch.numRows() > 0) {
          (0 until initial.columnarBatch.numCols()).foreach { colNo =>
            val initialFv = initial.columnarBatch.column(colNo).getArrowValueVector
            val currentFv = otherAr.columnarBatch.column(colNo).getArrowValueVector
            initialFv.append(currentFv)
            initial.columnarBatch.setNumRows(
              initial.columnarBatch.numRows() + otherAr.columnarBatch.numRows()
            )
          }
        }
        otherAr.arrowStreamReader.close(true)
      }
      Some(initial)
    }
  }

  final class ColBatchWithReader(
    val columnarBatch: ColumnarBatch,
    val arrowStreamReader: ArrowStreamReader
  )

  def deserialize(
    arr: Array[Byte]
  )(implicit bufferAllocator: BufferAllocator): ColBatchWithReader = {
    val byteArrayInputStream = new ByteArrayInputStream(arr)
    val arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, bufferAllocator)
    arrowStreamReader.loadNextBatch()
    val arrowColumnVectors =
      arrowStreamReader.getVectorSchemaRoot.getFieldVectors.asScala.map(vec =>
        new ArrowColumnVector(vec)
      )
    val valueCount =
      if (arrowColumnVectors.isEmpty) 0
      else
        arrowColumnVectors.head.readPrivate.accessor.vector.obj
          .asInstanceOf[FieldVector]
          .getValueCount

    val columnarBatch = new ColumnarBatch(arrowColumnVectors.toArray, valueCount)
    columnarBatch.setNumRows(valueCount)
    new ColBatchWithReader(columnarBatch, arrowStreamReader)
  }

  def serialize(input: ColumnarBatch): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val vectors = (0 until input.numCols())
      .map(idx =>
        input
          .column(idx)
          .asInstanceOf[ArrowColumnVector]
          .readPrivate
          .accessor
          .vector
          .obj
          .asInstanceOf[FieldVector]
      )
    try {
      serializeFixedLength(vectors.toList, outputStream)
      outputStream.flush()
      outputStream.toByteArray
    } finally outputStream.close()
  }

  private def serializeFixedLength(vec: List[FieldVector], stream: OutputStream): Unit = {
    val vectorSchemaRoot = new VectorSchemaRoot(vec.asJava)
    val arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null, stream)
    try arrowStreamWriter.writeBatch()
    finally arrowStreamWriter.close()
  }
}
