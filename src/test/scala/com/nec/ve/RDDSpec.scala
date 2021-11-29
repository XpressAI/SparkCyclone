package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.ve.RDDSpec.{doubleBatches, longBatches}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.{DeferredVeProcess, WrappingVeo}
import com.nec.ve.VeRDD.RichKeyedRDD
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.scalatest.freespec.AnyFreeSpec

final class RDDSpec extends AnyFreeSpec with SparkAdditions with VeKernelInfra {
  "We can pass around some Arrow things" in withSparkSession2(identity) { sparkSession =>
    longBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 500, step = 1, numSlices = 4)
    }
      .foreach(println)
  }

  "We can perform a VE call on Arrow things" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>
    implicit val veProc: VeProcess =
      DeferredVeProcess(() => WrappingVeo(SparkCycloneExecutorPlugin._veo_proc))
    val result = compiledWithHeaders(DoublingFunction.toCodeLinesNoHeaderOutPtr("f").cCode) {
      path =>
        val ref = veProc.loadLibrary(path)
        doubleBatches {
          sparkSession.sparkContext
            .range(start = 1, end = 500, step = 1, numSlices = 4)
            .map(_.toDouble)
        }.map(arrowVec => VeColVector.fromFloat8Vector(arrowVec))
          .map(ve => veProc.execute(ref, "f", List(ve), DoublingFunction.outputs.map(_.veType)))
          .map(vectors => {
            WithTestAllocator { implicit alloc =>
              val vec = vectors.head.toArrowVector().asInstanceOf[Float8Vector]
              try vec.toList
              finally vec.close()
            }
          })
          .collect()
          .toList
          .flatten
          .sorted
    }

    val expected = List.range(1, 500).map(_.toDouble).map(_ * 2)
    expect(result == expected)
  }

  "Exchange data across partitions" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>
    implicit val veProc: VeProcess =
      DeferredVeProcess(() => WrappingVeo(SparkCycloneExecutorPlugin._veo_proc))
    val MultiFunctionName = "f_multi"
    val result =
      compiledWithHeaders(PartitioningFunction.toCodeLinesNoHeaderOutPtr(MultiFunctionName).cCode) {
        path =>
          val ref = veProc.loadLibrary(path)
          doubleBatches {
            sparkSession.sparkContext
              .range(start = 1, end = 501, step = 1, numSlices = 4)
              .map(_.toDouble)
          }
            .mapPartitions(
              f = veIterator =>
                veIterator
                  .map(arrowVec => {
                    try VeColVector.fromFloat8Vector(arrowVec)
                    finally arrowVec.close()
                  })
                  .flatMap(vecv => {
                    try {
                      veProc
                        .executeMulti(
                          ref,
                          MultiFunctionName,
                          List(vecv),
                          List(CFunctionGeneration.VeScalarType.veNullableDouble)
                        )
                        .map { case (k, vs) => (k, vs.head) }
                    } finally vecv.free()
                  }),
              preservesPartitioning = true
            )
            .exchangeBetweenVEs()
            .mapPartitions(vectorIter =>
              Iterator
                .continually {
                  vectorIter.flatMap { vector =>
                    WithTestAllocator { implicit alloc =>
                      val vec = vector.toArrowVector().asInstanceOf[Float8Vector]
                      val vl = vec.toList
                      println(vl)
                      try if (vl.isEmpty) None else Some(vl.max)
                      finally vec.close()
                    }
                  }.max
                }
                .take(1)
            )
            .collect()
            .toList
      }

    val expected = Set[Double](199, 299, 399, 500)
    expect(result.toSet == expected)
  }

}

object RDDSpec {

  final case class NativeFunction(name: String)

  def longBatches(rdd: RDD[Long]): RDD[BigIntVector] = {
    rdd.mapPartitions(iteratorLong =>
      Iterator
        .continually {
          val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"allocator for longs", 0, Long.MaxValue)
          val theList = iteratorLong.toList
          val vec = new BigIntVector("input", allocator)
          theList.iterator.zipWithIndex.foreach { case (v, i) =>
            vec.setSafe(i, v)
          }
          vec.setValueCount(theList.size)
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            vec.close()
            allocator.close()
          }
          vec
        }
        .take(1)
    )
  }
  def doubleBatches(rdd: RDD[Double]): RDD[Float8Vector] = {
    rdd.mapPartitions(iteratorDouble =>
      Iterator
        .continually {
          val allocator: BufferAllocator = ArrowUtilsExposed.rootAllocator
            .newChildAllocator(s"allocator for floats", 0, Long.MaxValue)
          val theList = iteratorDouble.toList
          val vec = new Float8Vector("input", allocator)
          theList.iterator.zipWithIndex.foreach { case (v, i) =>
            vec.setSafe(i, v)
          }
          vec.setValueCount(theList.size)
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            vec.close()
            allocator.close()
          }
          vec
        }
        .take(1)
    )
  }

}
