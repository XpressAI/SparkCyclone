package io.sparkcyclone.data.conversion

import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnVectorConversions._
import io.sparkcyclone.data.{InputSamples, VeColVectorSource}
import io.sparkcyclone.spark.agile.core.VeScalarType
import scala.reflect.ClassTag
import scala.util.Random
import java.util.UUID
import io.sparkcyclone.util.FixedBitSet
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class SparkSqlColumnVectorConversionsUnitSpec extends AnyWordSpec {
  def runConversionTest[T : ClassTag](input: Seq[Option[T]], column: ColumnVector): Unit = {
    implicit val source = VeColVectorSource(s"${UUID.randomUUID}")
    val name = s"${UUID.randomUUID}"
    val colvec = column.toBytePointerColVector(name, input.size)

    // Check fields
    colvec.veType.scalaType should be (SparkToVeScalarTypeMap(column.dataType).scalaType)
    colvec.name should be (name)
    colvec.source should be (source)
    colvec.numItems should be (input.size)
    colvec.buffers.size should be (2)

    // Data buffer capacity should be correctly set
    colvec.buffers(0).capacity() should be (input.size.toLong * colvec.veType.asInstanceOf[VeScalarType].cSize)

    // Check validity buffer
    val validityBuffer = colvec.buffers(1)
    validityBuffer.capacity() should be ((input.size / 64.0).ceil.toLong * 8)
    val bitset = FixedBitSet.from(validityBuffer)
    0.until(input.size).foreach(i => bitset.get(i) should be (input(i).nonEmpty))

    // Check conversion
    colvec.toSeqOpt[T] should be (input)
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
      val input = InputSamples.seqOpt[Int]
      val column = newColumnVector(input.size, IntegerType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putInt(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert ShortType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[Short]
      val column = newColumnVector(input.size, ShortType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putShort(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert LongType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[Long]
      val column = newColumnVector(input.size, LongType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putLong(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert FloatType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[Float]
      val column = newColumnVector(input.size, FloatType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putFloat(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert DoubleType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[Double]
      val column = newColumnVector(input.size, DoubleType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putDouble(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert DateType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[Int]
      // The only difference between DateType and IntegerType ColumnVector is the type label
      val column = newColumnVector(input.size, DateType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putInt(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert TimestampType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[Long]
      val column = newColumnVector(input.size, TimestampType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putLong(i, v)
        case (None, i) => column.putNull(i)
      }

      runConversionTest(input, column)
    }

    "correctly convert StringType ColumnVector to BytePointerColVector" in {
      val input = InputSamples.seqOpt[String]
      val column = newColumnVector(input.size, StringType)

      input.zipWithIndex.foreach {
        case (Some(v), i) => column.putByteArray(i, v.getBytes)
        case (None, i) => column.putNull(i)
      }

      val name = s"${UUID.randomUUID}"
      val source = VeColVectorSource(s"${UUID.randomUUID}")
      val colvec = input.toBytePointerColVector(name)(source)

      // Check fields
      colvec.veType.scalaType should be (classOf[String])
      colvec.name should be (name)
      colvec.source should be(source)
      colvec.buffers.size should be (4)

      // Data, starts, and lens buffer capacities should be correctly set
      val capacity = input.foldLeft(0) { case (accum, x) =>
        accum + x.map(_.getBytes("UTF-32LE").size).getOrElse(0)
      }
      colvec.buffers(0).capacity() should be (capacity)
      colvec.buffers(1).capacity() should be (input.size.toLong * 4)
      colvec.buffers(2).capacity() should be (input.size.toLong * 4)

      // Check conversion - null String values should be preserved as well
      colvec.toSeqOpt[String] should be (input)
    }
  }
}
