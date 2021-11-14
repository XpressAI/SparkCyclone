package com.nec.spark.serialization

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.WithTestAllocator
import com.nec.cmake.functions.ParseCSVSpec.{RichFloat8, RichIntVector, RichVarCharVector}
import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector.RichObject
import com.nec.spark.serialization.ArrowColumnarBatchDeSerializerSpec.ValueInfo.{
  FloatStorage,
  IntStorage,
  StringStorage
}
import com.nec.spark.serialization.ArrowColumnarBatchDeSerializerSpec.{
  extractFieldVectors,
  genColB,
  ImmutableColBatch
}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, Float8Vector, IntVector, VarCharVector}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.Checkers

object ArrowColumnarBatchDeSerializerSpec {

  sealed trait ValueInfo[Value] {
    type Vector <: FieldVector
    def create(name: String)(implicit bufferAllocator: BufferAllocator): Vector
    def parse(vector: Vector): List[Option[Value]]
    def values: List[Option[Value]]
  }

  object ValueInfo {
    final case class IntStorage(ints: Option[Int]*) extends ValueInfo[Int] {
      override type Vector = IntVector
      override def create(name: String)(implicit bufferAllocator: BufferAllocator): IntVector = {
        val iv = new IntVector(name, bufferAllocator)
        iv.setValueCount(ints.size)
        values.zipWithIndex.foreach {
          case (None, idx)        => iv.setNull(idx)
          case (Some(value), idx) => iv.set(idx, value)
        }
        iv
      }
      override def parse(vector: IntVector): List[Option[Int]] = vector.toListSafe

      override def values: List[Option[Int]] = ints.toList
    }
    final case class StringStorage(strings: Option[String]*) extends ValueInfo[String] {
      override type Vector = VarCharVector
      override def create(
        name: String
      )(implicit bufferAllocator: BufferAllocator): VarCharVector = {
        val iv = new VarCharVector(name, bufferAllocator)
        iv.setValueCount(strings.size)
        values.zipWithIndex.foreach {
          case (None, idx)        => iv.setNull(idx)
          case (Some(value), idx) => iv.setSafe(idx, value.getBytes())
        }
        iv
      }
      override def parse(vector: VarCharVector): List[Option[String]] = vector.toListSafe

      override def values: List[Option[String]] = strings.toList
    }
    final case class FloatStorage(ints: Option[Double]*) extends ValueInfo[Double] {
      override type Vector = Float8Vector
      override def create(name: String)(implicit bufferAllocator: BufferAllocator): Float8Vector = {
        val iv = new Float8Vector(name, bufferAllocator)
        iv.setValueCount(ints.size)
        values.zipWithIndex.foreach {
          case (None, idx)        => iv.setNull(idx)
          case (Some(value), idx) => iv.set(idx, value)
        }
        iv
      }
      override def parse(vector: Float8Vector): List[Option[Double]] = vector.toListSafe

      override def values: List[Option[Double]] = ints.toList
    }
  }

  final class ColBatchWithVectors(
    val columnarBatch: ColumnarBatch,
    val arrowVectors: List[FieldVector]
  )

  final case class ImmutableColBatch(rowsCount: Int, columns: List[ValueInfo[_]]) {
    def toColumnarBatch(implicit allocator: BufferAllocator): ColBatchWithVectors = {
      val arrowCols = columns.zipWithIndex.map { case (vi, i) => vi.create(s"col_${i}") }
      new ColBatchWithVectors(
        new ColumnarBatch(arrowCols.map(vec => new ArrowColumnVector(vec)).toArray, rowsCount),
        arrowCols
      )
    }
  }

  def extractFieldVectors(columnarBatch: ColumnarBatch): List[FieldVector] = {
    (0 until columnarBatch.numCols())
      .map(i =>
        columnarBatch
          .column(i)
          .asInstanceOf[ArrowColumnVector]
          .readPrivate
          .accessor
          .vector
          .obj
          .asInstanceOf[FieldVector]
      )
      .toList
  }

  def intVectorGen(valuesCount: Int): Gen[IntStorage] =
    Gen
      .listOfN(valuesCount, Gen.option(Arbitrary.arbInt.arbitrary))
      .map(list => IntStorage(list: _*))
  def floatVectorGen(valuesCount: Int): Gen[FloatStorage] =
    Gen
      .listOfN(valuesCount, Gen.option(Arbitrary.arbDouble.arbitrary))
      .map(list => FloatStorage(list: _*))
  def varCharVectorGen(valuesCount: Int): Gen[StringStorage] =
    Gen
      .listOfN(valuesCount, Gen.option(Gen.alphaNumStr))
      .map(list => StringStorage(list: _*))

  def someFieldVectorGen(valuesCount: Int): Gen[ValueInfo[_]] =
    Gen.oneOf(intVectorGen(valuesCount), floatVectorGen(valuesCount), varCharVectorGen(valuesCount))

  val genColB: Gen[ImmutableColBatch] = {
    for {
      numCols <- Gen.chooseNum(0, 19)
      numRows <- Gen.chooseNum(0, 20)
      cols <- Gen.listOfN(numCols, someFieldVectorGen(numRows))
    } yield ImmutableColBatch(numRows, cols)
  }

}
final class ArrowColumnarBatchDeSerializerSpec extends AnyFreeSpec with Checkers {

  "It works" in {
    WithTestAllocator { implicit allocator =>
      val p: Prop = Prop.forAll(genColB) { immutableColBatch =>
        val clz = immutableColBatch.toColumnarBatch
        val cb = clz.columnarBatch
        try {
          val byteArray = ArrowColumnarBatchDeSerializer.serialize(cb)
          val colBatchWithReader = ArrowColumnarBatchDeSerializer.deserialize(byteArray)
          val gotCols: List[FieldVector] = extractFieldVectors(colBatchWithReader.columnarBatch)
          try {
            val gotColsStr = gotCols.toString()
            val avsStr = clz.arrowVectors.toString()
            gotColsStr == avsStr
          } finally {
            colBatchWithReader.columnarBatch.close()
            colBatchWithReader.arrowStreamReader.close(true)
          }
        } finally clz.arrowVectors.foreach(_.close())
      }
      check(p)
    }
  }

  "Check one reduced case" in {
    val icb = ImmutableColBatch(
      rowsCount = 1,
      columns = List(
        StringStorage(None),
        StringStorage(None),
        StringStorage(Some("hVh")),
        StringStorage(None)
      )
    )
    WithTestAllocator { implicit alloc =>
      val clz = icb.toColumnarBatch
      val cb = clz.columnarBatch
      val ba = ArrowColumnarBatchDeSerializer.serialize(cb)
      val otherDb = ArrowColumnarBatchDeSerializer.deserialize(ba)

      val gotCols = extractFieldVectors(otherDb.columnarBatch).toString()
      val expt = clz.arrowVectors.toString()

      try expect(gotCols == expt)
      finally {
        otherDb.columnarBatch.close()
        otherDb.arrowStreamReader.close(true)
      }
    }
  }

//  behavior of "ArrowColumnarBatchDeSerializer"
//
//  it should "correctly serialize ColumnarBatchWithIntVector" in {
//    withDirectIntVector(Seq(1, 2, 3, 4, 10)) { vec =>
//      val batch = new ColumnarBatch(Array(new ArrowColumnVector(vec)), 5)
//      val serializer = ArrowColumnarBatchDeSerializer
//      val arr = serializer.serialize(batch)
//      val deserializedBatch = serializer.deserialize(arr)
//      val outputVec = deserializedBatch
//        .column(0)
//        .asInstanceOf[AccessibleArrowColumnVector]
//        .getArrowValueVector
//        .asInstanceOf[IntVector]
//
//      outputVec.getValueCount shouldBe vec.getValueCount
//      outputVec.valueSeq shouldBe vec.valueSeq
//      outputVec.validitySeq shouldBe vec.validitySeq
//    }
//  }
//
//  it should "correctly serialize IntVector with null values" in {
//    withNullableIntVector(Seq(Some(1), Some(2), Some(3), None, Some(5), None)) { vec =>
//      val batch = new ColumnarBatch(Array(new ArrowColumnVector(vec)), 5)
//      val serializer = ArrowColumnarBatchDeSerializer
//      val arr = serializer.serialize(batch)
//      val deserializedBatch = serializer.deserialize(arr)
//      val outputVec = deserializedBatch
//        .column(0)
//        .asInstanceOf[AccessibleArrowColumnVector]
//        .getArrowValueVector
//        .asInstanceOf[IntVector]
//
//      outputVec.getValueCount shouldBe vec.getValueCount
//      outputVec.valueSeq shouldBe vec.valueSeq
//      outputVec.validitySeq shouldBe vec.validitySeq
//    }
//  }
//
//  it should "correctly serialize multiple Columns with null values" in {
//    withNullableIntVector(Seq(Some(1), Some(2), Some(3), None, Some(5), None)) { vec =>
//      val batch = new ColumnarBatch(
//        Array(new ArrowColumnVector(vec), new ArrowColumnVector(vec)),
//        5
//      )
//      val serializer = ArrowColumnarBatchDeSerializer
//      val arr = serializer.serialize(batch)
//      val deserializedBatch = serializer.deserialize(arr)
//      val outputVec = deserializedBatch
//        .column(0)
//        .asInstanceOf[AccessibleArrowColumnVector]
//        .getArrowValueVector
//        .asInstanceOf[IntVector]
//      val outputVec2 = deserializedBatch
//        .column(1)
//        .asInstanceOf[AccessibleArrowColumnVector]
//        .getArrowValueVector
//        .asInstanceOf[IntVector]
//
//      outputVec.getValueCount shouldBe vec.getValueCount
//      outputVec.valueSeq shouldBe vec.valueSeq
//      outputVec.validitySeq shouldBe vec.validitySeq
//      outputVec2.getValueCount shouldBe vec.getValueCount
//      outputVec2.valueSeq shouldBe vec.valueSeq
//      outputVec2.validitySeq shouldBe vec.validitySeq
//    }
//  }
//
//  it should "correctly serialize VarCharVector" in {
//    val inputSeq = Seq("First", "Second", "Third", "Fourth")
//    withArrowStringVector(inputSeq) { vec =>
//      val batch = new ColumnarBatch(Array(new ArrowColumnVector(vec)), 5)
//      val serializer = ArrowColumnarBatchDeSerializer
//      val arr = serializer.serialize(batch)
//      val deserializedBatch = serializer.deserialize(arr)
//      val outputVec = deserializedBatch
//        .column(0)
//        .asInstanceOf[AccessibleArrowColumnVector]
//        .getArrowValueVector
//        .asInstanceOf[VarCharVector]
//
//      outputVec.getValueCount shouldBe vec.getValueCount
//      outputVec.valueSeq shouldBe vec.valueSeq
//      outputVec.validitySeq shouldBe vec.validitySeq
//      outputVec.offsetSeq shouldBe vec.offsetSeq
//    }
//  }
//
//  it should "correctly serialize VarCharVector with null values" in {
//    val inputSeq = Seq(Some("First"), Some("Second"), None, None, Some("End"))
//    withNullableArrowStringVector(inputSeq) { vec =>
//      val batch = new ColumnarBatch(Array(new ArrowColumnVector(vec)), 5)
//      val serializer = ArrowColumnarBatchDeSerializer
//      val arr = serializer.serialize(batch)
//      val deserializedBatch = serializer.deserialize(arr)
//      val outputVec = deserializedBatch
//        .column(0)
//        .asInstanceOf[AccessibleArrowColumnVector]
//        .getArrowValueVector
//        .asInstanceOf[VarCharVector]
//
//      outputVec.getValueCount shouldBe vec.getValueCount
//      outputVec.valueSeq shouldBe vec.valueSeq
//      outputVec.validitySeq shouldBe vec.validitySeq
//      outputVec.offsetSeq shouldBe vec.offsetSeq
//    }
//  }

}
