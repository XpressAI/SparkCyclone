package com.nec.cmake
import com.nec.arrow.ArrowVectorBuilders
import com.nec.arrow.TransferDefinitions
import com.nec.arrow.native.Float8GenericNativeIf
import com.nec.arrow.native.Float8GenericNativeIf.InVh
import com.nec.ve.FlexiPassingVESpec
import com.nec.ve.FlexiPassingVESpec.Add1Mul2
import com.nec.ve.FlexiPassingVESpec.RichFloat8
import org.scalatest.freespec.AnyFreeSpec

object FlexiPassingCSpec {}
final class FlexiPassingCSpec extends AnyFreeSpec {
  "Through Arrow, it works" in {
    val libPath = CMakeBuilder.buildC(
      List(TransferDefinitions.TransferDefinitionsSourceCode, FlexiPassingVESpec.Sources)
        .mkString("\n\n")
    )
    val result = ArrowVectorBuilders.withDirectFloat8Vector(List(1, 2, 3)) { vcv =>
      Add1Mul2.call(Float8GenericNativeIf.NativeIf(libPath))(InVh(vcv)).contents.toList
    }
    assert(result == List[Double](4, 6, 8))
  }
}
