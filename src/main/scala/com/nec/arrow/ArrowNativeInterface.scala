package com.nec.arrow
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector

trait ArrowNativeInterface extends Serializable {
  def callFunction(
    name: String,
    inputArguments: List[Option[VarCharVector]],
    outputArguments: List[Option[IntVector]]
  )
}
