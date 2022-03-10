package com.nec.ve

import scala.reflect.ClassTag
import scala.language.implicitConversions
import org.apache.spark.rdd.RDD

class VectorizedRDD[T](rdd: RDD[T]) {

  def veMap[U:ClassTag](f: (T) => U ): RDD[U] = {
    rdd.map[U](f)
  }
}

object VectorizedRDD {
  implicit def rddToVectorizedRDD[T](r: RDD[T]) = new VectorizedRDD(r)
}