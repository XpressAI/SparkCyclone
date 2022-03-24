package com.nec.arrow.colvector

import com.nec.arrow.colvector.ArrayTConversions._
import com.nec.arrow.colvector.ArrowVectorConversions._
import com.nec.arrow.colvector.SparkSqlColumnVectorConversions._
import com.nec.spark.agile.core.VeScalarType
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import java.util.BitSet
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class SparkSqlColumnVectorConversionsUnitSpec extends AnyWordSpec {
  def runConversionTest[T : ClassTag](input: Seq[T], column: ColumnVector): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val name = s"${UUID.randomUUID}"
    val colvec = column.toBytePointerColVector(name, input.size)

    // Check fields
    colvec.underlying.veType.scalaType should be (SparkToVeScalarTypeMap(column.dataType).scalaType)
    colvec.underlying.name should be (name)
    colvec.underlying.source should be (source)
    colvec.underlying.numItems should be (input.size)
    colvec.underlying.buffers.size should be (2)

    // Data buffer capacity should be correctly set
    colvec.underlying.buffers(0).get.capacity() should be (input.size.toLong * colvec.underlying.veType.asInstanceOf[VeScalarType].cSize)

    // Check validity buffer
    val validityBuffer = colvec.underlying.buffers(1).get
    validityBuffer.capacity() should be ((input.size / 8.0).ceil.toLong)
    val bitset = BitSet.valueOf(validityBuffer.asBuffer)
    0.until(input.size).foreach(i => bitset.get(i) should be (true))

    // Check conversion
    colvec.toArray[T] should be (input)
  }

  def newColumnVector(size: Int, dtype: DataType): WritableColumnVector = {
    if (Math.random < 0.5) {
      new OnHeapColumnVector(size, dtype)
    } else {
      new OffHeapColumnVector(size, dtype)
    }
  }

  "SparkSqlColumnVectorConversions" should {
    "correctly convert IntegerType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000))
      val column = newColumnVector(input.size, IntegerType)
      input.zipWithIndex.foreach { case (v, i) => column.putInt(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert ShortType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000).toShort)
      val column = newColumnVector(input.size, ShortType)
      input.zipWithIndex.foreach { case (v, i) => column.putShort(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert LongType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextLong)
      val column = newColumnVector(input.size, LongType)
      input.zipWithIndex.foreach { case (v, i) => column.putLong(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert FloatType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextFloat * 1000)
      val column = newColumnVector(input.size, FloatType)
      input.zipWithIndex.foreach { case (v, i) => column.putFloat(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert DoubleType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextDouble * 1000)
      val column = newColumnVector(input.size, DoubleType)
      input.zipWithIndex.foreach { case (v, i) => column.putDouble(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert DateType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextInt(10000))
      val column = newColumnVector(input.size, DateType)
      input.zipWithIndex.foreach { case (v, i) => column.putInt(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert TimestampType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextLong)
      val column = newColumnVector(input.size, TimestampType)
      input.zipWithIndex.foreach { case (v, i) => column.putLong(i, v) }

      runConversionTest(input, column)
    }

    "correctly convert StringType ColumnVector to BytePointerColVector" in {
      val input = 0.until(Random.nextInt(100)).map(_ => Random.nextString(Random.nextInt(30))).toArray
      val column = newColumnVector(input.size, StringType)
      input.zipWithIndex.foreach { case (v, i) => column.putByteArray(2, v.getBytes) }

      // Set one of the values to null
      if (input.size > 0) {
        val i = Random.nextInt(input.size)
        input(i) = null
        // `putNotNull(i)` will need to be called before value-setting can work once again
        column.putNull(i)
      }

      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check fields
      colvec.underlying.veType.scalaType should be (classOf[String])
      colvec.underlying.name should be (name)
      colvec.underlying.source should be(source)
      colvec.underlying.buffers.size should be (4)

      // Data, starts, and lens buffer capacities should be correctly set
      val capacity = input.foldLeft(0) { case (accum, x) =>
        accum + (if (x == null) 0 else x.getBytes("UTF-32LE").size)
      }
      colvec.underlying.buffers(0).get.capacity() should be (capacity)
      colvec.underlying.buffers(1).get.capacity() should be (input.size.toLong * 4)
      colvec.underlying.buffers(2).get.capacity() should be (input.size.toLong * 4)

      // Check conversion - null String values should be preserved as well
      colvec.toArray[String] should be (input)
    }
  }
}
