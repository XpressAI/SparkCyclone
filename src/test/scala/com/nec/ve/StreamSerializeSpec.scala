package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.colvector.{GenericColVector, UnitColVector}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{VeNullableDouble, VeNullableInt}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeSerializer.{
  VeDeserializationStream,
  VeSerializationStream,
  VeSerializedContainer
}
import org.scalatest.freespec.AnyFreeSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

final class StreamSerializeSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  import OriginalCallingContext.Automatic._

  "Sample Bytes can be read out" in {
    val outUcv =
      UnitColVector(GenericColVector(source, 99, "o_dbl", None, VeNullableDouble, (), List((), ())))
    val outBytes: Array[Byte] = Array(-80, 105, -12, 0, 16, 96, 0, 0, -88, 76, -64, 4, 12, 96, 0, 0,
      0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 16, 64, 0, 0, 0, 0, 0, 0, 20, 64, 0, 0, 0, 0, 0, 0,
      24, 64, 0, 0, 0, 0, 0, 0, 28, 64, 0, 0, 0, 0, 0, 0, 32, 64, 0, 0, 0, 0, 0, 0, 34, 64, 0, 0, 0,
      0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 38, 64, 0, 0, 0, 0, 0, 0, 40, 64, 0, 0, 0, 0, 0, 0, 42, 64,
      0, 0, 0, 0, 0, 0, 44, 64, 0, 0, 0, 0, 0, 0, 46, 64, 0, 0, 0, 0, 0, 0, 48, 64, 0, 0, 0, 0, 0,
      0, 49, 64, 0, 0, 0, 0, 0, 0, 50, 64, 0, 0, 0, 0, 0, 0, 51, 64, 0, 0, 0, 0, 0, 0, 52, 64, 0, 0,
      0, 0, 0, 0, 53, 64, 0, 0, 0, 0, 0, 0, 54, 64, 0, 0, 0, 0, 0, 0, 55, 64, 0, 0, 0, 0, 0, 0, 56,
      64, 0, 0, 0, 0, 0, 0, 57, 64, 0, 0, 0, 0, 0, 0, 58, 64, 0, 0, 0, 0, 0, 0, 59, 64, 0, 0, 0, 0,
      0, 0, 60, 64, 0, 0, 0, 0, 0, 0, 61, 64, 0, 0, 0, 0, 0, 0, 62, 64, 0, 0, 0, 0, 0, 0, 63, 64, 0,
      0, 0, 0, 0, 0, 64, 64, 0, 0, 0, 0, 0, -128, 64, 64, 0, 0, 0, 0, 0, 0, 65, 64, 0, 0, 0, 0, 0,
      -128, 65, 64, 0, 0, 0, 0, 0, 0, 66, 64, 0, 0, 0, 0, 0, -128, 66, 64, 0, 0, 0, 0, 0, 0, 67, 64,
      0, 0, 0, 0, 0, -128, 67, 64, 0, 0, 0, 0, 0, 0, 68, 64, 0, 0, 0, 0, 0, -128, 68, 64, 0, 0, 0,
      0, 0, 0, 69, 64, 0, 0, 0, 0, 0, -128, 69, 64, 0, 0, 0, 0, 0, 0, 70, 64, 0, 0, 0, 0, 0, -128,
      70, 64, 0, 0, 0, 0, 0, 0, 71, 64, 0, 0, 0, 0, 0, -128, 71, 64, 0, 0, 0, 0, 0, 0, 72, 64, 0, 0,
      0, 0, 0, -128, 72, 64, 0, 0, 0, 0, 0, 0, 73, 64, 0, 0, 0, 0, 0, -128, 73, 64, 0, 0, 0, 0, 0,
      0, 74, 64, 0, 0, 0, 0, 0, -128, 74, 64, 0, 0, 0, 0, 0, 0, 75, 64, 0, 0, 0, 0, 0, -128, 75, 64,
      0, 0, 0, 0, 0, 0, 76, 64, 0, 0, 0, 0, 0, -128, 76, 64, 0, 0, 0, 0, 0, 0, 77, 64, 0, 0, 0, 0,
      0, -128, 77, 64, 0, 0, 0, 0, 0, 0, 78, 64, 0, 0, 0, 0, 0, -128, 78, 64, 0, 0, 0, 0, 0, 0, 79,
      64, 0, 0, 0, 0, 0, -128, 79, 64, 0, 0, 0, 0, 0, 0, 80, 64, 0, 0, 0, 0, 0, 64, 80, 64, 0, 0, 0,
      0, 0, -128, 80, 64, 0, 0, 0, 0, 0, -64, 80, 64, 0, 0, 0, 0, 0, 0, 81, 64, 0, 0, 0, 0, 0, 64,
      81, 64, 0, 0, 0, 0, 0, -128, 81, 64, 0, 0, 0, 0, 0, -64, 81, 64, 0, 0, 0, 0, 0, 0, 82, 64, 0,
      0, 0, 0, 0, 64, 82, 64, 0, 0, 0, 0, 0, -128, 82, 64, 0, 0, 0, 0, 0, -64, 82, 64, 0, 0, 0, 0,
      0, 0, 83, 64, 0, 0, 0, 0, 0, 64, 83, 64, 0, 0, 0, 0, 0, -128, 83, 64, 0, 0, 0, 0, 0, -64, 83,
      64, 0, 0, 0, 0, 0, 0, 84, 64, 0, 0, 0, 0, 0, 64, 84, 64, 0, 0, 0, 0, 0, -128, 84, 64, 0, 0, 0,
      0, 0, -64, 84, 64, 0, 0, 0, 0, 0, 0, 85, 64, 0, 0, 0, 0, 0, 64, 85, 64, 0, 0, 0, 0, 0, -128,
      85, 64, 0, 0, 0, 0, 0, -64, 85, 64, 0, 0, 0, 0, 0, 0, 86, 64, 0, 0, 0, 0, 0, 64, 86, 64, 0, 0,
      0, 0, 0, -128, 86, 64, 0, 0, 0, 0, 0, -64, 86, 64, 0, 0, 0, 0, 0, 0, 87, 64, 0, 0, 0, 0, 0,
      64, 87, 64, 0, 0, 0, 0, 0, -128, 87, 64, 0, 0, 0, 0, 0, -64, 87, 64, 0, 0, 0, 0, 0, 0, 88, 64,
      0, 0, 0, 0, 0, 64, 88, 64, 0, 0, 0, 0, 0, -128, 88, 64, 32, 3, 0, 0, 0, 0, 0, 0, -32, -124,
      -12, 0, 16, 96, 0, 0, -1, -1, -1, -1, 15, 96, 0, 0)
    WithTestAllocator { implicit alloc =>
      val acv = outUcv.deserialize(outBytes).toArrowVector()
      println(acv.toString)
    }
  }

  "Unit vec can be serialized/deserialized" in {
    val ucv = UnitColVector(
      GenericColVector(source, 9, "test", Some(123), VeNullableInt, (), buffers = List((), ()))
    )
    val ucvBytes = ucv.byteForm
    val gotBackUcv = UnitColVector.fromBytes(ucvBytes)
    assert(gotBackUcv == ucv)
  }

  "Simple serialization works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector = VeColVector.fromArrowVector(f8v)
        val unitVec = colVec.underlying.toUnit
        val resVec = unitVec.deserialize(colVec.serialize())
        val gotVecStr = resVec.toArrowVector().toString
        val expectedVecStr = f8v.toString

        assert(gotVecStr == expectedVecStr)
      }
    }
  }

  "Simple serialization of a batch works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColBatch = VeColBatch.fromList(List(VeColVector.fromArrowVector(f8v)))
        val bytes = colVec.serializeToBytes()
        val theBatch = VeColBatch.readFromBytes(bytes)
        val gotVecStr = theBatch.cols.head.toArrowVector().toString
        val expectedVecStr = f8v.toString

        assert(gotVecStr == expectedVecStr)
      }
    }
  }

}
