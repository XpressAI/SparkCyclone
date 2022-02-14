package com.nec.ve.serializer

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.arrow.WithTestAllocator
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.nec.ve.{VeColBatch, VeKernelInfra, WithVeProcess}
import org.scalatest.freespec.AnyFreeSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

final class VeSerializerSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

  "Simple serialization of a batch of 2 columns works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        withArrowFloat8VectorI(List(-1, -2, -3)) { f8v2 =>
          val colVec: VeColBatch = VeColBatch.fromList(
            List(VeColVector.fromArrowVector(f8v), VeColVector.fromArrowVector(f8v2))
          )
          val theBatch = VeColBatch.deserialize(colVec.serialize())
          val gotVecStr = theBatch.cols.head.toArrowVector().toString
          val gotVecStr2 = theBatch.cols.drop(1).head.toArrowVector().toString
          val expectedVecStr = f8v.toString
          val expectedVecStr2 = f8v2.toString

          expect(gotVecStr == expectedVecStr, gotVecStr2 == expectedVecStr2)
        }
      }
    }
  }

  "Stream serialization of a batch of 2 columns works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        withArrowFloat8VectorI(List(-1, -2, -3)) { f8v2 =>
          val colVec: VeColBatch = VeColBatch.fromList(
            List(VeColVector.fromArrowVector(f8v), VeColVector.fromArrowVector(f8v2))
          )
          val byteArrayOutputStream = new ByteArrayOutputStream()
          val dataOutputStream = new DataOutputStream(byteArrayOutputStream)
          try colVec.serializeToStream(dataOutputStream)
          finally {
            byteArrayOutputStream.flush()
            byteArrayOutputStream.close()
          }
          val bytes = byteArrayOutputStream.toByteArray
          val byteArrayInputStream = new ByteArrayInputStream(bytes)
          val dataInputStream = new DataInputStream(byteArrayInputStream)
          val theBatch = VeColBatch.fromStream(dataInputStream)
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
