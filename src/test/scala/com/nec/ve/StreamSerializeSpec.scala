package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.colvector.{GenericColVector, UnitColVector}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableInt
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
  "StringStreamSerialization works" in {
    WithTestAllocator { implicit alloc =>
      val ls = List(None, Some("Xyz"), Some("asbc"))
      withNullableArrowStringVector(ls) { strVec =>
        val colVec: VeColVector = VeColVector.fromArrowVector(strVec)
        val unitVec = colVec.underlying.toUnit
        val outStream = new ByteArrayOutputStream()
        colVec.serializeToStream(outStream)
        outStream.flush()
        val inStream = new ByteArrayInputStream(outStream.toByteArray)
        val resVec = unitVec.deserializeFromStream(inStream)
        val gotVecStr = resVec.toArrowVector().toString
        val expectedVecStr = strVec.toString

        assert(gotVecStr == expectedVecStr)
      }
    }
  }

  "Simple serialization of a batch of 2 columns works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        withArrowFloat8VectorI(List(-1, -2, -3)) { f8v2 =>
          val colVec: VeColBatch = VeColBatch.fromList(
            List(VeColVector.fromArrowVector(f8v), VeColVector.fromArrowVector(f8v2))
          )
          val bytes = colVec.serializeToBytes()
          val theBatch = VeColBatch.readFromBytes(bytes)
          val gotVecStr = theBatch.cols.head.toArrowVector().toString
          val gotVecStr2 = theBatch.cols.drop(1).head.toArrowVector().toString
          val expectedVecStr = f8v.toString
          val expectedVecStr2 = f8v2.toString

          expect(gotVecStr == expectedVecStr, gotVecStr2 == expectedVecStr2)
        }
      }
    }
  }

}
