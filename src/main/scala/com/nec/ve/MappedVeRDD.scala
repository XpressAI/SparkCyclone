package com.nec.ve

import com.nec.native.CompiledVeFunction

import scala.language.experimental.macros
import scala.reflect.ClassTag

class MappedVeRDD[U: ClassTag, T: ClassTag](
  rdd: VeRDD[T],
  func: CompiledVeFunction,
) extends ChainedVeRDD[U](rdd, func) {}
