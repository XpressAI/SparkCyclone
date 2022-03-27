package com.nec.ve

import com.nec.spark.agile.core.{CFunction2, CVector}

import scala.language.experimental.macros
import scala.reflect.ClassTag

class FilteredVeRDD[T: ClassTag](
  rdd: VeRDD[T],
  func: CFunction2,
  soPath: String,
  outputs: List[CVector]) extends ChainedVeRDD[T](rdd, func, soPath, outputs) {
}

