package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.WithTestAllocator
import com.nec.arrow.colvector.{GenericColVector, UnitColVector}
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableInt
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

  "We can serialize and deserialize a VeColBatch" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector = VeColVector.fromArrowVector(f8v)

        val baos = new ByteArrayOutputStream()
        val serializationStream = new VeSerializationStream(baos, cleanUpInput = true)

        val thingsToWrite = List(
          VeSerializedContainer.JavaLangInteger(1000000),
          VeSerializedContainer.VeColBatchToSerialize(VeColBatch.fromList(List(colVec))),
          VeSerializedContainer.JavaLangInteger(9),
          VeSerializedContainer.JavaLangInteger(10),
          VeSerializedContainer.VeColBatchToSerialize(VeColBatch.fromList(List(colVec)))
        )

        val expectedResults =
          List[String](1000000.toString, f8v.toString, 9.toString, 10.toString, f8v.toString)

        thingsToWrite.foreach(serializationStream.writeContainer)
        serializationStream.flush()
        val byteArray = baos.toByteArray
        serializationStream.close()

        val bais = new ByteArrayInputStream(byteArray)
        val deser = new VeDeserializationStream(bais)
        val gotList: List[String] = Iterator
          .fill(5)(deser.readOut())
          .map {
            case VeSerializedContainer.VeColBatchToSerialize(cb) =>
              val vec = cb.cols.head.toArrowVector()
              try vec.toString
              finally vec.close()
            case VeSerializedContainer.JavaLangInteger(n) => n.toString
          }
          .toList

        colVec.free()
        expect(gotList == expectedResults)
      }
    }
  }
}
