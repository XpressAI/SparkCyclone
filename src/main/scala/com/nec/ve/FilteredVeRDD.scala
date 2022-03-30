package com.nec.ve

import com.nec.native.CompiledVeFunction

import scala.language.experimental.macros
import scala.reflect.ClassTag

class FilteredVeRDD[T: ClassTag](
  rdd: VeRDD[T],
  func: CompiledVeFunction) extends ChainedVeRDD[T](rdd, func) {
}

