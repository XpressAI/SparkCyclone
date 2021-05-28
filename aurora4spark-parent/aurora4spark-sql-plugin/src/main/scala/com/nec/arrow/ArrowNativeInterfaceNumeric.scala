package com.nec.arrow
import org.apache.arrow.vector.Float8Vector

trait ArrowNativeInterfaceNumeric extends Serializable {
  def callFunction(
    name: String,
    inputArguments: List[Option[Float8Vector]],
    outputArguments: List[Option[Float8Vector]]
  )
}
