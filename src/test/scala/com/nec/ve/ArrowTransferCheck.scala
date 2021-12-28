package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders._
import com.nec.arrow.WithTestAllocator
import com.nec.spark.SparkCycloneExecutorPlugin.source
import com.nec.spark.agile.CFunctionGeneration.VeScalarType.VeNullableDouble
import com.nec.spark.agile.CFunctionGeneration.{VeScalarType, VeString}
import com.nec.util.RichVectors.{RichFloat8, RichVarCharVector}
import com.nec.ve.GroupingFunction.DataDescription
import com.nec.ve.GroupingFunction.DataDescription.KeyOrValue
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.ve.VeColBatch.{VeBatchOfBatches, VeColVector}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, Float8Vector, ValueVector, VarCharVector}
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

  "Partition data by some means (simple Int partitioning in this case) (PIN)" in {
    compiledWithHeaders(
      GroupingFunction
        .groupData(
          data = List(
            DataDescription(VeScalarType.VeNullableDouble, KeyOrValue.Key),
            DataDescription(VeString, KeyOrValue.Key),
            DataDescription(VeScalarType.VeNullableDouble, KeyOrValue.Value)
          ),
          totalBuckets = 2
        )
        .toCodeLines("f")
        .cCode
    ) { path =>
      val lib = veProcess.loadLibrary(path)
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          withArrowFloat8VectorI(List(9, 8, 7)) { f8v2 =>
            val lastString = "cccc"
            withNullableArrowStringVector(List("a", "b", lastString).map(Some.apply)) { sv =>
              val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
              val colVec2: VeColVector = VeColVector.fromFloat8Vector(f8v2)
              val colVecS: VeColVector = VeColVector.fromVarcharVector(sv)
              val results = veProcess.executeMulti(
                libraryReference = lib,
                functionName = "f",
                cols = List(colVec, colVecS, colVec2),
                results =
                  List(VeScalarType.veNullableDouble, VeString, VeScalarType.veNullableDouble)
              )

              val plainResultsD: List[(Int, List[(Double, String, Double)])] = results.map {
                case (index, vecs) =>
                  val vecFloat = vecs(0).toArrowVector().asInstanceOf[Float8Vector]
                  val vecStr = vecs(1).toArrowVector().asInstanceOf[VarCharVector]
                  val vecFl2 = vecs(2).toArrowVector().asInstanceOf[Float8Vector]
                  try {
                    index -> vecFloat.toList.zip(vecFl2.toList).zip(vecStr.toList).map {
                      case ((a, b), c) => (a, c, b)
                    }
                  } finally {
                    vecStr.close()
                    vecFloat.close()
                    vecFl2.close()
                  }
              }

              val allSets = plainResultsD.flatMap(_._2).toSet

              val expectedGroups: Set[(Double, String, Double)] =
                Set((1, "a", 9), (2, "b", 8), (3, lastString, 7))

              assert(
                plainResultsD.map(_._2.size).toSet == Set(1, 2),
                "We expect the groups to have exactly size 2 and 1 each because of the split"
              )
              assert(allSets == expectedGroups, "we verify that we get back the data we had put in")
            }
          }
        }
      }
    }
  }

  "We can serialize/deserialize VeColVector" - {

    def checkVector(
      valueVector: ValueVector
    )(implicit veProcess: VeProcess, bufferAllocator: BufferAllocator): Unit = {
      val colVec: VeColVector = VeColVector.fromArrowVector(valueVector)
      val serialized = colVec.serialize()
      val serList = serialized.toList
      val newColVec = colVec.deserialize(serialized)
      expect(
        newColVec.containerLocation != colVec.containerLocation,
        newColVec.bufferLocations != colVec.bufferLocations
      )
      val newSerialized = newColVec.serialize().toList
      val newSerList = newSerialized.toList
      assert(newSerList == serList, "Serializing a deserialized one should yield the same result")
      val newColVecArrow = newColVec.toArrowVector()
      try {
        colVec.free()
        newColVec.free()
        expect(newColVecArrow.toString == valueVector.toString)
      } finally newColVecArrow.close()

    }

    "for Float8Vector" in {
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          checkVector(f8v)
        }
      }
    }
    "for IntVector" in {
      WithTestAllocator { implicit alloc =>
        withDirectIntVector(List(1, 2, 3)) { f8v =>
          checkVector(f8v)
        }
      }
    }
    "for BigIntVector" in {
      WithTestAllocator { implicit alloc =>
        withDirectBigIntVector(List(1, 2, 3)) { f8v =>
          checkVector(f8v)
        }
      }
    }
    "for VarCharVector" in {
      WithTestAllocator { implicit alloc =>
        withArrowStringVector(List("1", "2", "3x")) { sv =>
          checkVector(sv)
        }
      }
    }
    "for empty VarCharVector" in {
      WithTestAllocator { implicit alloc =>
        withArrowStringVector(List.empty) { sv =>
          checkVector(sv)
        }
      }
    }
    "for an empty Float8Vector" in {
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List.empty) { f8v =>
          checkVector(f8v)
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
        withArrowFloat8VectorI(List(1, 2, 3, -1)) { f8v =>
          withArrowStringVector(Seq("a", "b", "c", "x")) { sv =>
            withArrowStringVector(Seq("d", "e", "f")) { sv2 =>
              withArrowFloat8VectorI(List(2, 3, 4)) { f8v2 =>
                val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
                val colVec2: VeColVector = VeColVector.fromFloat8Vector(f8v2)
                val sVec: VeColVector = VeColVector.fromVarcharVector(sv)
                val sVec2: VeColVector = VeColVector.fromVarcharVector(sv2)
                val colBatch1: VeColBatch = VeColBatch(colVec.numItems, List(colVec, sVec))
                val colBatch2: VeColBatch = VeColBatch(colVec2.numItems, List(colVec2, sVec2))
                val bg = VeBatchOfBatches.fromVeColBatches(List(colBatch1, colBatch2))
                val r: List[VeColVector] = veProcess.executeMultiIn(
                  libraryReference = lib,
                  functionName = fName,
                  batches = bg,
                  results = colBatch1.cols.map(_.veType)
                )

                val resultVecs: List[FieldVector] = r.map(_.toArrowVector())

                try {
                  val nums = resultVecs(0).asInstanceOf[Float8Vector].toListSafe
                  val strs = resultVecs(1).asInstanceOf[VarCharVector].toList

                  val expected = List(1, 2, 3, -1, 2, 3, 4).map(v => Option(v))
                  val expectedStrs = Seq("a", "b", "c", "x", "d", "e", "f")
                  expect(nums == expected, strs == expectedStrs)
                } finally resultVecs.foreach(_.close())
              }
            }
          }
        }
      }
    }
  }
}
