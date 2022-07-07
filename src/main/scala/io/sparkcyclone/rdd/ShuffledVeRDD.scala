package io.sparkcyclone.rdd

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class ShuffledVeRDD[K: ClassTag, V: ClassTag, C: ClassTag](
  @transient var prev: VeRDD[_],
  part: Partitioner) {
  def setKeyOrdering(ordering: Ordering[K]): RDD[(K, V)] = ???
}