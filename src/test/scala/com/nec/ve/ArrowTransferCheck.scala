package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.{
  withArrowFloat8VectorI,
  withArrowStringVector,
  withNullableArrowStringVector
}
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, VeScalarType}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowTransferCheck extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  "Identify check: data that we put into the VE can be retrieved back out" - {
    "for Float8Vector" in {
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val arrowVec = colVec.toArrowVector()

          try {
            colVec.free()
            expect(arrowVec.toString == f8v.toString)
          } finally arrowVec.close()
        }
      }
    }

    "for VarCharVector" in {
      WithTestAllocator { implicit alloc =>
        withArrowStringVector(List("Quick", "brown", "fox", "smth smth", "lazy dog")) { f8v =>
          val colVec: VeColVector = VeColVector.fromVarcharVector(f8v)
          val arrowVec = colVec.toArrowVector()

          try {
            colVec.free()
            expect(arrowVec.toString == f8v.toString)
          } finally arrowVec.close()
        }
      }
    }
  }

  "Execute our function" in {
    compiledWithHeaders(DoublingFunction.toCodeLinesNoHeaderOutPtr("f").cCode) { path =>
      val lib = veProcess.loadLibrary(path)
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val results = veProcess.execute(
            libraryReference = lib,
            functionName = "f",
            cols = List(colVec),
            results = List(VeScalarType.veNullableDouble)
          )
          expect(results.size == 1)
          val vec = results.head.toArrowVector().asInstanceOf[Float8Vector]
          val result = vec.toList
          try expect(result == List[Double](2, 4, 6))
          finally vec.close()
        }
      }
    }
  }

  "Execute multi-function" in {
    compiledWithHeaders(PartitioningFunction.toCodeLinesNoHeaderOutPtr("f").cCode) { path =>
      val lib = veProcess.loadLibrary(path)
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(95, 99, 105, 500, 501)) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val results = veProcess.executeMulti(
            libraryReference = lib,
            functionName = "f",
            cols = List(colVec),
            results = List(VeScalarType.veNullableDouble)
          )

          val plainResults: List[(Int, Option[Double])] = results.map { case (index, vecs) =>
            val vec = vecs.head
            index -> {
              val av = vec.toArrowVector().asInstanceOf[Float8Vector]
              val avl = av.toList
              try if (avl.isEmpty) None else Some(avl.max)
              finally av.close()
            }
          }

          val expectedResult: List[(Int, Option[Double])] =
            List((0, Some(99)), (1, Some(105)), (2, None), (3, None), (4, Some(501)))

          expect(plainResults == expectedResult)
        }
      }
    }
  }

  "We can serialize/deserialize VeColVector" - {
    "for Float8Vector" in {
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val serialized = colVec.serialize()
          val serList = serialized.toList
          val newColVec = colVec.deserialize(serialized)
          expect(
            newColVec.containerLocation != colVec.containerLocation,
            newColVec.bufferLocations != colVec.bufferLocations
          )
          val newSerialized = newColVec.serialize().toList
          val newSerList = newSerialized.toList
          assert(
            newSerList == serList,
            "Serializing a deserialized one should yield the same result"
          )
          val newColVecArrow = newColVec.toArrowVector()
          try {
            colVec.free()
            newColVec.free()
            expect(newColVecArrow.toString == f8v.toString)
          } finally newColVecArrow.close()
        }
      }
    }
    "for an empty Float8Vector" in {
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List.empty) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val serialized = colVec.serialize()
          val serList = serialized.toList
          val newColVec = colVec.deserialize(serialized)
          expect(
            newColVec.containerLocation != colVec.containerLocation,
            newColVec.bufferLocations != colVec.bufferLocations
          )
          val newSerialized = newColVec.serialize().toList
          val newSerList = newSerialized.toList
          assert(
            newSerList == serList,
            "Serializing a deserialized one should yield the same result"
          )
          val newColVecArrow = newColVec.toArrowVector()
          try {
            colVec.free()
            newColVec.free()
            expect(newColVecArrow.toString == f8v.toString)
          } finally newColVecArrow.close()
        }
      }
    }
  }
}
