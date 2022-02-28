package com.nec.arrow.colvector

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import com.nec.arrow.CatsArrowVectorBuilders
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector
import org.apache.spark.sql.types.{IntegerType, StructField}
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

final class ColVectorTransformSpec extends AnyFreeSpec with BeforeAndAfterAll {
  import java.time.{Instant, LocalDate}
  private implicit val bufAllocator: RootAllocator = new RootAllocator()
  private val vecBuilder = CatsArrowVectorBuilders(Ref.unsafe(1))
  List(
    ("Optional Int", vecBuilder.optionalIntVector(Seq(Some(1), None, Some(3)))),
    ("Short", vecBuilder.shortVector(Seq(1, 2, 3))),
    ("Long", vecBuilder.longVector(Seq(1, 2, 3))),
    (
      "DateTime",
      vecBuilder.dateVector(
        Seq(
          LocalDate.parse("2019-01-02"),
          LocalDate.parse("2019-01-03"),
          LocalDate.parse("2019-01-04")
        )
      )
    ),
    ("Double", vecBuilder.doubleVector(Seq(1, 2, 3))),
    (
      "Timestamp",
      vecBuilder.timestampVector(
        Seq(Instant.now(), Instant.now().plusSeconds(1), Instant.now().plusSeconds(2))
      )
    ),
    ("String", vecBuilder.stringVector(Seq("A", "BB", "CDEF")))
  ).foreach { case (name, resource) =>
    s"It works for ${name}" in {
      resource
        .use { vector =>
          IO.delay {
            implicit val veColVectorSource: VeColVectorSource = VeColVectorSource("test")
            val result = BytePointerColVector
              .fromColumnarVectorViaArrow(
                name = "test",
                columnVector = new ArrowColumnVector(vector),
                size = vector.getValueCount
              )
              .map { case (originalFieldVector, bytePointerColVector) =>
                try {
                  val fieldVector = bytePointerColVector.toArrowVector()
                  try fieldVector.toString
                  finally fieldVector.close()
                } finally originalFieldVector.close()
              }

            val expectedVec = vector.toString
            assert(result.contains(expectedVec))
          }
        }
        .unsafeRunSync()
    }
  }

  "Off-heap Int works" in {
    val Array(col) = OffHeapColumnVector.allocateColumns(
      3,
      Array(StructField("int", IntegerType, nullable = false))
    )
    col.putInt(0, 1)
    col.putInt(1, -200000)
    col.putInt(2, 99000)

    val gotVecStr =
      try {
        implicit val veColVectorSource: VeColVectorSource = VeColVectorSource("test")
        val bytePointerColVector: BytePointerColVector = BytePointerColVector
          .fromOffHeapColumnarVector(size = 3, str = "int", col = col)
          .getOrElse(fail("Could not convert"))

        val fieldVector = bytePointerColVector.toArrowVector()
        try fieldVector.toString
        finally fieldVector.close()
      } finally col.close()

    assert(gotVecStr == "[1, -200000, 99000]")
  }

  override protected def afterAll(): Unit = {
    bufAllocator.close()
    super.afterAll()
  }
}
