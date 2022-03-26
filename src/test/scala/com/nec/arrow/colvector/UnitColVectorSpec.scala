package com.nec.arrow.colvector

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.agile.core.VeNullableInt
import com.nec.ve.colvector.VeColBatch.VeColVectorSource
import org.scalatest.freespec.AnyFreeSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

final class UnitColVectorSpec extends AnyFreeSpec {
  val ucv = UnitColVector(
    GenericColVector(
      VeColVectorSource("tested"),
      9,
      "test",
      Some(123),
      VeNullableInt,
      (),
      buffers = List((), ())
    )
  )

  "It works" in {
    val baos = new ByteArrayOutputStream()
    val daos = new DataOutputStream(baos)
    try ucv.toStreamFast(daos)
    finally daos.close()

    val bytes: Array[Byte] = baos.toByteArray

    val bais = new ByteArrayInputStream(bytes)
    val dais = new DataInputStream(bais)
    val ucvOut = UnitColVector.fromStreamFast(dais)
    expect(ucvOut == ucv, ucv.streamedSize == bytes.length)
  }
}
