package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.colvector.{GenericColVector, UnitColVector}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.{VeNullableDouble, VeNullableInt}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import org.scalatest.freespec.AnyFreeSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

final class StreamSerializeSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  import OriginalCallingContext.Automatic._

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

  "Simple StreamSerialization works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector = VeColVector.fromArrowVector(f8v)
        val unitVec = colVec.underlying.toUnit
        val outStream = new ByteArrayOutputStream()
        colVec.serializeToStream(outStream)
        outStream.flush()
        val inStream = new ByteArrayInputStream(outStream.toByteArray)
        val resVec = unitVec.deserializeFromStream(inStream)
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
