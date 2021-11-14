package com.nec.spark.serialization

import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector.RichObject
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

object ArrowColumnarBatchDeSerializer extends Serializable {
  def deserialize(arr: Array[Byte])(implicit bufferAllocator: BufferAllocator): ColumnarBatch = {
    val byteArrayInputStream = new ByteArrayInputStream(arr)
    val arrowReader = new ArrowStreamReader(byteArrayInputStream, bufferAllocator)
    try {
      arrowReader.loadNextBatch()
      val arrowColumnVectors =
        arrowReader.getVectorSchemaRoot.getFieldVectors.asScala.map(vec =>
          new ArrowColumnVector(vec)
        )
      val valueCount =
        if (arrowColumnVectors.isEmpty) 0
        else
          arrowColumnVectors.head.readPrivate.accessor.vector.obj
            .asInstanceOf[FieldVector]
            .getValueCount
      new ColumnarBatch(arrowColumnVectors.toArray, valueCount)
    } finally arrowReader.close(true)
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
