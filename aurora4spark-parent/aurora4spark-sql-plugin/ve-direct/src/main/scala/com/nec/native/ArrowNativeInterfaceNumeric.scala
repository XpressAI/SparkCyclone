package com.nec.native

import org.apache.arrow.vector.{Float8Vector, IntVector, VarCharVector}

trait ArrowNativeInterfaceNumeric {
  def callFunction(
                    name: String,
                    inputArguments: List[Option[Float8Vector]],
                    outputArguments: List[Option[Float8Vector]]
  )
}
