package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.{
  withArrowFloat8VectorI,
  withArrowStringVector,
  withDirectBigIntVector,
  withDirectIntVector,
  withNullableArrowStringVector
}
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableDouble
import com.nec.spark.agile.CFunctionGeneration.{CFunction, VeScalarType, VeString, VeType}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.RichVectors.{RichFloat8, RichVarCharVector}
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.ve.VeColBatch.{VeBatchOfBatches, VeColVector}
import org.apache.arrow.vector.{FieldVector, Float8Vector, VarCharVector}
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
    "for BigInt" in {
      WithTestAllocator { implicit alloc =>
        withDirectBigIntVector(List(1, -1, 1238)) { biv =>
          val colVec: VeColVector = VeColVector.fromBigIntVector(biv)
          val arrowVec = colVec.toArrowVector()

          try {
            colVec.free()
            expect(arrowVec.toString == biv.toString)
          } finally arrowVec.close()
        }
      }
    }
    "for Int" in {
      WithTestAllocator { implicit alloc =>
        withDirectIntVector(List(1, 2, 3, -5)) { dirInt =>
          val colVec: VeColVector = VeColVector.fromIntVector(dirInt)
          val arrowVec = colVec.toArrowVector()

          try {
            colVec.free()
            expect(arrowVec.toString == dirInt.toString)
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

  /**
   * Let's first take the data, as it is,
   * perform partial aggregation,
   * then bucket it,
   * then exchange it,
   * re-merge according to buckets
   * then finalize
   */

  "We can merge multiple VeColBatches" in {
    val fName = "merger"

    compiledWithHeaders(
      MergerFunction.merge(types = List(VeNullableDouble, VeString)).toCodeLines(fName).cCode
    ) { path =>
      val lib = veProcess.loadLibrary(path)
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          withArrowStringVector(Seq("a", "b", "c")) { sv =>
            withArrowStringVector(Seq("d", "e", "f")) { sv2 =>
              withArrowFloat8VectorI(List(2, 3, 4)) { f8v2 =>
                val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
                val colVec2: VeColVector = VeColVector.fromFloat8Vector(f8v2)
                val sVec: VeColVector = VeColVector.fromVarcharVector(sv)
                val sVec2: VeColVector = VeColVector.fromVarcharVector(sv2)
                val colBatch1: VeColBatch = VeColBatch(3, List(colVec, sVec))
                val colBatch2: VeColBatch = VeColBatch(3, List(colVec2, sVec2))
                val r: List[VeColVector] = veProcess.executeMultiIn(
                  libraryReference = lib,
                  functionName = fName,
                  batches = VeBatchOfBatches.fromVeColBatches(List(colBatch1, colBatch2)),
                  results = colBatch1.cols.map(_.veType)
                )

                val resultVecs: List[FieldVector] = r.map(_.toArrowVector())

                try {
                  val nums = resultVecs(0).asInstanceOf[Float8Vector].toList
                  val strs = resultVecs(1).asInstanceOf[VarCharVector].toList

                  expect(nums == List(1, 2, 3, 2, 3, 4), strs == List("a", "b", "c", "d", "e", "f"))
                } finally resultVecs.foreach(_.close())
              }
            }
          }
        }
      }
    }
  }
}
