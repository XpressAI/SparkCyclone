package com.nec.spark.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.nec.arrow.AccessibleArrowColumnVector
import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.RichVectors.RichIntVector
import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.vectorized.ColumnarBatch

class ArrowColumnarBatchDeSerializerSpec extends AnyFlatSpec with Matchers {

  behavior of "ArrowColumnarBatchDeSerializer"

  it should "correctly serialize ColumnarBatchWithIntVector" in {
    withDirectIntVector(Seq(1, 2, 3, 4, 10)){ vec =>
      val batch = new ColumnarBatch(Array(new AccessibleArrowColumnVector(vec)), 5)
      val serializer = new ArrowColumnarBatchDeSerializer()
      val arr =  serializer.serialize(batch)
      val deserializedBatch = serializer.deserialize(arr)
      val outputVec = deserializedBatch
        .column(0)
        .asInstanceOf[AccessibleArrowColumnVector]
        .getArrowValueVector
        .asInstanceOf[IntVector]

      outputVec.getValueCount shouldBe vec.getValueCount
      outputVec.valueSeq shouldBe vec.valueSeq
      outputVec.validitySeq shouldBe vec.validitySeq
    }
  }

  it should "correctly serialize IntVector with null values" in {
    withNullableIntVector(Seq(Some(1), Some(2), Some(3), None, Some(5), None)){ vec =>
      val batch = new ColumnarBatch(Array(new AccessibleArrowColumnVector(vec)), 5)
      val serializer = new ArrowColumnarBatchDeSerializer()
      val arr =  serializer.serialize(batch)
      val deserializedBatch = serializer.deserialize(arr)
      val outputVec = deserializedBatch
        .column(0)
        .asInstanceOf[AccessibleArrowColumnVector]
        .getArrowValueVector
        .asInstanceOf[IntVector]

      outputVec.getValueCount shouldBe vec.getValueCount
      outputVec.valueSeq shouldBe vec.valueSeq
      outputVec.validitySeq shouldBe vec.validitySeq
    }
  }

  it should "correctly serialize multiple Columns with null values" in {
    withNullableIntVector(Seq(Some(1), Some(2), Some(3), None, Some(5), None)){ vec =>
      val batch = new ColumnarBatch(Array(new AccessibleArrowColumnVector(vec), new AccessibleArrowColumnVector(vec)), 5)
      val serializer = new ArrowColumnarBatchDeSerializer()
      val arr =  serializer.serialize(batch)
      val deserializedBatch = serializer.deserialize(arr)
      val outputVec = deserializedBatch
        .column(0)
        .asInstanceOf[AccessibleArrowColumnVector]
        .getArrowValueVector
        .asInstanceOf[IntVector]
      val outputVec2 = deserializedBatch
        .column(1)
        .asInstanceOf[AccessibleArrowColumnVector]
        .getArrowValueVector
        .asInstanceOf[IntVector]

      outputVec.getValueCount shouldBe vec.getValueCount
      outputVec.valueSeq shouldBe vec.valueSeq
      outputVec.validitySeq shouldBe vec.validitySeq
      outputVec2.getValueCount shouldBe vec.getValueCount
      outputVec2.valueSeq shouldBe vec.valueSeq
      outputVec2.validitySeq shouldBe vec.validitySeq
    }
  }

  it should "correctly serialize VarCharVector" in {
    val inputSeq = Seq("First", "Second", "Third", "Fourth")
    withArrowStringVector(inputSeq){ vec =>
      val batch = new ColumnarBatch(Array(new AccessibleArrowColumnVector(vec)), 5)
      val serializer = new ArrowColumnarBatchDeSerializer()
      val arr =  serializer.serialize(batch)
      val deserializedBatch = serializer.deserialize(arr)
      val outputVec = deserializedBatch
        .column(0)
        .asInstanceOf[AccessibleArrowColumnVector]
        .getArrowValueVector
        .asInstanceOf[VarCharVector]

      outputVec.getValueCount shouldBe vec.getValueCount
      outputVec.valueSeq shouldBe vec.valueSeq
      outputVec.validitySeq shouldBe vec.validitySeq
      outputVec.offsetSeq shouldBe vec.offsetSeq
    }
  }

  it should "correctly serialize VarCharVector with null values" in {
    val inputSeq = Seq(Some("First"), Some("Second"), None, None, Some("End"))
    withNullableArrowStringVector(inputSeq){ vec =>
      val batch = new ColumnarBatch(Array(new AccessibleArrowColumnVector(vec)), 5)
      val serializer = new ArrowColumnarBatchDeSerializer()
      val arr =  serializer.serialize(batch)
      val deserializedBatch = serializer.deserialize(arr)
      val outputVec = deserializedBatch
        .column(0)
        .asInstanceOf[AccessibleArrowColumnVector]
        .getArrowValueVector
        .asInstanceOf[VarCharVector]

      outputVec.getValueCount shouldBe vec.getValueCount
      outputVec.valueSeq shouldBe vec.valueSeq
      outputVec.validitySeq shouldBe vec.validitySeq
      outputVec.offsetSeq shouldBe vec.offsetSeq
    }
  }

}
