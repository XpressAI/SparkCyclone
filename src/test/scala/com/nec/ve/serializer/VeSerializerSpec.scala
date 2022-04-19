package com.nec.ve.serializer

import com.eed3si9n.expecty.Expecty.expect
import com.nec.colvector.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.colvector.{VeColVector, WithTestAllocator}
import com.nec.colvector.ArrowVectorConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.ve.serializer.DualBatchOrBytes.ColBatchWrapper
import com.nec.ve.{VeKernelInfra, WithVeProcess}
import com.nec.colvector.VeColBatch
import org.scalatest.freespec.AnyFreeSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

@VectorEngineTest
final class VeSerializerSpec extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  import com.nec.ve.VeProcess.OriginalCallingContext.Automatic._

  "Simple serialization of a batch of 2 columns works" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        withArrowFloat8VectorI(List(-1, -2, -3)) { f8v2 =>
          val colVec: VeColBatch = VeColBatch.fromList(
            List(f8v.toVeColVector, f8v2.toVeColVector)
          )
          val theBatch = VeColBatch.deserialize(colVec.serialize())
          val gotVecStr = theBatch.cols.head.toBytePointerColVector.toArrowVector.toString
          val gotVecStr2 = theBatch.cols.drop(1).head.toBytePointerColVector.toArrowVector.toString
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
            List(f8v.toVeColVector, f8v2.toVeColVector)
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
          val gotVecStr = theBatch.cols.head.toBytePointerColVector.toArrowVector.toString
          val gotVecStr2 = theBatch.cols.drop(1).head.toBytePointerColVector.toArrowVector.toString
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
            List(f8v.toVeColVector, f8v2.toVeColVector)
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
          val gotVecStr = gotBatch.cols.head.toBytePointerColVector.toArrowVector.toString
          val gotVecStr2 = gotBatch.cols.drop(1).head.toBytePointerColVector.toArrowVector.toString
          val expectedVecStr = f8v.toString
          val expectedVecStr2 = f8v2.toString

          expect(gotVecStr == expectedVecStr, gotVecStr2 == expectedVecStr2)
        }
      }
    }
  }
}
