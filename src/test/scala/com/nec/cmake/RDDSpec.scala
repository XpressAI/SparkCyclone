package com.nec.cmake

import com.nec.cmake.RDDSpec.{doubleBatches, longBatches}
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.spark.agile.CFunctionGeneration.VeType
import com.nec.ve.{DynamicVeSqlExpressionEvaluationSpec, VeColBatch}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.{DeferredVeProcess, WrappingVeo}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, FieldVector, Float8Vector}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.scalatest.freespec.AnyFreeSpec

object RDDSpec {

  final case class NativeFunction(name: String)

  implicit class RichColBatchRDD(rdd: RDD[VeColBatch]) {

    /**
     * @param f function that takes '<partition index>, <number of partitions>, <input_columns>, <output_columns>'
     * @return repartitioned set of VeColBatch
     */
    def exchange(f: Long): RDD[VeColBatch] = {
      ???
    }

    /**
     * @param f function that takes '<input_columns>', '<output_columns>'
     * @return newly mapped batches
     */
    def mapBatch(f: Long): RDD[VeColBatch] = {
      ???
    }

    /**
     * @param f function that takes '<input_columns>', '<input_columns>', '<output_columns>'
     * @return newly mapped batches
     */
    def reduce(f: Long): RDD[VeColBatch] = {
      ???
    }
  }

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

final class RDDSpec extends AnyFreeSpec with SparkAdditions {
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
    doubleBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 500, step = 1, numSlices = 4)
        .map(_.toDouble)
    }.map(arrowVec =>
      VeColVector.fromFloat8Vector(arrowVec)(
        DeferredVeProcess(() => WrappingVeo(SparkCycloneExecutorPlugin._veo_proc))
      )
    ).map(ve => )

  }
}
