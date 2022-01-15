package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.PureVeFunctions.DoublingFunction
import com.nec.ve.VERDDSpec.{doubleBatches, longBatches}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeRDD.RichKeyedRDD
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, Float8Vector}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

final class VERDDSpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  import OriginalCallingContext.Automatic._

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
    import SparkCycloneExecutorPlugin._
    val result = compiledWithHeaders(DoublingFunction.toCodeLinesNoHeaderOutPtr("f").cCode) {
      path =>
        val ref = veProcess.loadLibrary(path)
        doubleBatches {
          sparkSession.sparkContext
            .range(start = 1, end = 500, step = 1, numSlices = 4)
            .map(_.toDouble)
        }.map(arrowVec => VeColVector.fromArrowVector(arrowVec))
          .map(ve => veProcess.execute(ref, "f", List(ve), DoublingFunction.outputs.map(_.veType)))
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
}

object VERDDSpec {
  val MultiFunctionName = "f_multi"
  import OriginalCallingContext.Automatic._

  def exchangeBatches(sparkSession: SparkSession, pathStr: String): RDD[Double] = {

    import SparkCycloneExecutorPlugin.{veProcess => veProc}

    doubleBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 501, step = 1, numSlices = 4)
        .map(_.toDouble)
    }
      .mapPartitions(
        f = veIterator =>
          veIterator
            .map(arrowVec => {
              import SparkCycloneExecutorPlugin.source
              try VeColVector.fromArrowVector(arrowVec)
              finally arrowVec.close()
            })
            .flatMap(veColVector => {
              import SparkCycloneExecutorPlugin.source
              try {
                val ref = veProc.loadLibrary(java.nio.file.Paths.get(pathStr))

                veProc
                  .executeMulti(
                    ref,
                    MultiFunctionName,
                    List(veColVector),
                    List(CFunctionGeneration.VeScalarType.veNullableDouble)
                  )
                  .map { case (k, vs) => (k, vs.head) }
              } finally veColVector.free()
            }),
        preservesPartitioning = true
      )
      .exchangeBetweenVEs()
      .mapPartitions(vectorIter =>
        Iterator
          .continually {
            vectorIter.flatMap { vector =>
              WithTestAllocator { implicit alloc =>
                import SparkCycloneExecutorPlugin.source
                try {
                  val vec = vector.toArrowVector().asInstanceOf[Float8Vector]
                  val vl = vec.toList
                  try if (vl.isEmpty) None else Some(vl.max)
                  finally vec.close()
                } finally vector.free()
              }
            }.max
          }
          .take(1)
      )
  }

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
