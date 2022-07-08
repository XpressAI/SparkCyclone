package io.sparkcyclone.spark.codegen.core

import io.sparkcyclone.native.NativeFunction
import io.sparkcyclone.spark.transformation.VeFunction
import io.sparkcyclone.spark.transformation.VeFunctionStatus

trait VeFunctionTemplate extends NativeFunction {
  /*
    This is the function whose compiled symbol will be invoked by the VE process
    as part of the execution of a Spark Plan.
  */
  final def primary: CFunction2 = {
    toCFunction
  }

  /*
    A list of column vectors that is expected to be returned by invocation of
    this NativeFunction
  */
  def outputs: Seq[CVector]

  def toCFunction: CFunction2

  final def toVeFunction: VeFunction = {
    VeFunction(
      // Pass self (NativeFunction) in as the SourceCode
      VeFunctionStatus.SourceCode(this),
      identifier,
      outputs
    )
  }
}
