package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.WithTestAllocator
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

  "We can serialize and deserialize a VeColBatch" in {
    WithTestAllocator { implicit alloc =>
      withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
        val colVec: VeColVector = VeColVector.fromArrowVector(f8v)

        val baos = new ByteArrayOutputStream()
        val serializationStream = new VeSerializationStream(baos)

        val thingsToWrite = List(
          VeSerializedContainer.JavaLangInteger(5),
          VeSerializedContainer.VeColBatchToSerialize(VeColBatch.fromList(List(colVec))),
          VeSerializedContainer.JavaLangInteger(9),
          VeSerializedContainer.JavaLangInteger(10),
          VeSerializedContainer.VeColBatchToSerialize(VeColBatch.fromList(List(colVec)))
        )

        val expectedResults =
          List[String](5.toString, f8v.toString, 9.toString, 10.toString, f8v.toString)

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
