package io.sparkcyclone.data.conversion

import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.conversion.SparkSqlColumnarBatchConversions._
import io.sparkcyclone.data.{ColumnBatchEncoding, InputSamples, VeColVectorSource}
import java.util.UUID
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class SparkSqlColumnarBatchConversionsUnitSpec extends AnyWordSpec {
  val allocator = new RootAllocator(Int.MaxValue)
  implicit val source = VeColVectorSource(s"${UUID.randomUUID}")

  "SparkSqlColumnarBatchConversions" should {
    "correctly convert Spark SQL ColumnarBatch to BytePointerColBatch" in {
      val (raw1, input1) = {
        val raw = InputSamples.seqOpt[Short]
        val input = new SmallIntVector(s"${UUID.randomUUID}", allocator)
        input.setValueCount(raw.length)
        raw.zipWithIndex.foreach {
          case (Some(v), i) => input.set(i, v)
          case (None, i)    => input.setNull(i)
        }
        (raw, input)
      }

      val (raw2, input2) = {
        val raw = InputSamples.seqOpt[Double]
        val input = new Float8Vector(s"${UUID.randomUUID}", allocator)
        input.setValueCount(raw.length)
        raw.zipWithIndex.foreach {
          case (Some(v), i) => input.set(i, v)
          case (None, i)    => input.setNull(i)
        }
        (raw, input)
      }

      val (raw3, input3) = {
        val raw = InputSamples.seqOpt[String]
        val input = new VarCharVector(s"${UUID.randomUUID}", allocator)
        input.allocateNew()
        input.setValueCount(raw.length)
        raw.zipWithIndex.foreach {
          case (Some(v), i) => input.set(i, v.getBytes)
          case (None, i)    => input.setNull(i)
        }
        (raw, input)
      }

      // Create the ColumnarBatch
      val colbatch = new ColumnarBatch(Seq(input1, input2, input3).map(new ArrowColumnVector(_)).toArray)

      // Convert to BytePointerColBatch
      val schema = ColumnBatchEncoding.default.makeArrowSchema(colbatch.attributes)
      val bpbatch = colbatch.toBytePointerColBatch(schema)

      // Column values should be the same as the input
      bpbatch.columns(0).toSeqOpt[Short] should be (raw1)
      bpbatch.columns(1).toSeqOpt[Double] should be (raw2)
      bpbatch.columns(2).toSeqOpt[String] should be (raw3)
    }
  }
}
