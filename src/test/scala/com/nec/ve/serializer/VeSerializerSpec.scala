package com.nec.ve.serializer

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.arrow.colvector.ArrowVectorConversions._
import com.nec.arrow.WithTestAllocator
import com.nec.ve.colvector.VeColBatch.VeColVector
import com.nec.ve.serializer.DualBatchOrBytes.ColBatchWrapper
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
          val gotVecStr = theBatch.cols.head.toBytePointerVector.toArrowVector.toString
          val gotVecStr2 = theBatch.cols.drop(1).head.toBytePointerVector.toArrowVector.toString
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
          val veColBatch: VeColBatch = VeColBatch.fromList(
            List(VeColVector.fromArrowVector(f8v), VeColVector.fromArrowVector(f8v2))
          )
          val byteArrayOutputStream = new ByteArrayOutputStream()
          val dataOutputStream = new DataOutputStream(byteArrayOutputStream)
          try veColBatch.serializeToStream(dataOutputStream)
          finally {
            byteArrayOutputStream.flush()
            byteArrayOutputStream.close()
          }
          val bytes = byteArrayOutputStream.toByteArray
          val byteArrayInputStream = new ByteArrayInputStream(bytes)
          val dataInputStream = new DataInputStream(byteArrayInputStream)
          val theBatch = VeColBatch.fromStream(dataInputStream)
          val gotVecStr = theBatch.cols.head.toBytePointerVector.toArrowVector.toString
          val gotVecStr2 = theBatch.cols.drop(1).head.toBytePointerVector.toArrowVector.toString
          val expectedVecStr = f8v.toString
          val expectedVecStr2 = f8v2.toString

          expect(gotVecStr == expectedVecStr, gotVecStr2 == expectedVecStr2)
        }
      }
    }
  }

  "Check serializer fully" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        withArrowFloat8VectorI(List(-1, -2, -3)) { f8v2 =>
          val veColBatch: VeColBatch = VeColBatch.fromList(
            List(VeColVector.fromArrowVector(f8v), VeColVector.fromArrowVector(f8v2))
          )

          val byteArrayOutputStream = new ByteArrayOutputStream()
          val serStr = new VeSerializationStream(byteArrayOutputStream)
          serStr.writeObject(ColBatchWrapper(veColBatch))
          serStr.flush()
          serStr.close()

          val byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray)
          val deserStr = new VeDeserializationStream(byteArrayInputStream)
          val bytesOnly = deserStr
            .readObject[DualBatchOrBytes]()
            .fold(bytesOnly => bytesOnly, _ => sys.error(s"Got col batch, expected bytes"))
          val gotBatch =
            VeColBatch.fromStream(new DataInputStream(new ByteArrayInputStream(bytesOnly.bytes)))
          val gotVecStr = gotBatch.cols.head.toBytePointerVector.toArrowVector.toString
          val gotVecStr2 = gotBatch.cols.drop(1).head.toBytePointerVector.toArrowVector.toString
          val expectedVecStr = f8v.toString
          val expectedVecStr2 = f8v2.toString

          expect(gotVecStr == expectedVecStr, gotVecStr2 == expectedVecStr2)
        }
      }
    }
  }

}
