package com.nec.cmake

import com.nec.spark.SparkAdditions
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.scalatest.freespec.AnyFreeSpec

object RDDSpec {
  final case class VeColVector(dataType: DataType, veLocation: Long)
  final case class VeColBatch(numRows: Int, cols: List[VeColVector])

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
}

final class RDDSpec extends AnyFreeSpec with SparkAdditions {
  "We can pass around some Arrow things" in withSparkSession2(identity) { sparkSession =>
    sparkSession.sparkContext
      .range(start = 1, end = 500, step = 1, numSlices = 4)
      .mapPartitions(iteratorLong =>
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
      .foreach(println)
  }

  "We can perform a VE call" in {}
}
