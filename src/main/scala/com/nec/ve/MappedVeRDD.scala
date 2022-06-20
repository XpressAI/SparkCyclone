package com.nec.ve

import com.nec.native.transpiler.CompiledVeFunction

import scala.language.experimental.macros
import scala.reflect.runtime.universe.TypeTag

class MappedVeRDD[U: TypeTag, T: TypeTag](
  rdd: VeRDD[T],
  func: CompiledVeFunction,
) extends ChainedVeRDD[U](rdd, func) {}
