package com.nec.cmake

import com.nec.spark.SparkAdditions
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.spark.TaskContext
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.scalatest.freespec.AnyFreeSpec

final class RDDSpec extends AnyFreeSpec with SparkAdditions {
  "it works" in withSparkSession2(identity) { sparkSession =>
    sparkSession.sparkContext
      .range(1, 500)
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
}
