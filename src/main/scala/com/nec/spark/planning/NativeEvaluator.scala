package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric

trait NativeEvaluator extends Serializable {
  def forCode(code: String): ArrowNativeInterfaceNumeric
}
