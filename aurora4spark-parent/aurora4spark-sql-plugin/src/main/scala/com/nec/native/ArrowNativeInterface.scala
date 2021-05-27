package com.nec.native

import org.apache.arrow.vector.{IntVector, VarCharVector}

trait ArrowNativeInterface {
  def callFunction(
    name: String,
    inputArguments: List[Option[VarCharVector]],
    outputArguments: List[Option[IntVector]]
  )
}
