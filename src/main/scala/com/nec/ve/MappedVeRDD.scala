package com.nec.ve

import scala.reflect.ClassTag

class MappedVeRDD[T: ClassTag](rdd: VeRDD[T]) extends VeRDD[T](rdd) {
  override def reduce(f: (T, T) => T): T = {
    ???
  }
}
