package com.nec.ve

import com.nec.native.transpiler.CompiledVeFunction

import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class FilteredVeRDD[T: ClassTag: TypeTag](
  rdd: VeRDD[T],
  func: CompiledVeFunction) extends ChainedVeRDD[T](rdd, func) {
}
