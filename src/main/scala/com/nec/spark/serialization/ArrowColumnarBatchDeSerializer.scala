package com.nec.spark.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}

import scala.collection.JavaConverters.{seqAsJavaListConverter, collectionAsScalaIterableConverter, seqAsJavaList}

import com.nec.arrow.AccessibleArrowColumnVector
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}

import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch

class ArrowColumnarBatchDeSerializer extends Serializable {
  def deserialize(arr: Array[Byte]): ColumnarBatch = {
    val in = new ByteArrayInputStream(arr)
    val arrowReader = new ArrowStreamReader(in, ArrowUtilsExposed.rootAllocator)
    arrowReader.loadNextBatch()
    val vectors = arrowReader.getVectorSchemaRoot.getFieldVectors.asScala.map(vec => new AccessibleArrowColumnVector(vec))
    val valueCount = vectors.head.getArrowValueVector.getValueCount

    new ColumnarBatch(vectors.toArray, valueCount)
  }

  def serialize(input: ColumnarBatch): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val vectors = (0 until input.numCols())
      .map(idx => input.column(idx).asInstanceOf[AccessibleArrowColumnVector].getArrowValueVector.asInstanceOf[FieldVector])
    serializeFixedLength(vectors.toList, outputStream)
    outputStream.flush()
    outputStream.toByteArray
  }

  private def serializeFixedLength(vec: List[FieldVector], stream: OutputStream) = {
    val vectorSchemaRoot = new VectorSchemaRoot(vec.asJava)
    val arrowStreamWriter = new ArrowStreamWriter(vectorSchemaRoot, null, stream)
    arrowStreamWriter.writeBatch()
  }
}
