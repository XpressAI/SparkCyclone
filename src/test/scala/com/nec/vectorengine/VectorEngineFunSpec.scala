package com.nec.vectorengine

import com.nec.colvector.BytePointerColVector
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core.VeNullableDouble
import com.nec.util.CallContextOps._
import com.nec.ve.VeKernelInfra
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VectorEngineFunSpec extends AnyWordSpec with WithVeProcess with VeKernelInfra {
  var vengine: VectorEngine = _

  "VectorEngine" should {
    "correctly execute a basic VE function" in {
      val func = SampleVeFunctions.DoublingFunction

      compiledWithHeaders(func) { path =>
        val lib = process.load(path)
        val colvec = Seq[Double](1, 2, 3).map(Some(_)).toBytePointerColVector("_").toVeColVector2

        val outputs = vengine.execute(
          lib,
          func.name,
          inputs = Seq(colvec),
          outputs = List(VeNullableDouble.makeCVector("output"))
        )

        outputs.map(_.toBytePointerColVector2.toSeqOpt[Double].flatten) should be (Seq(Seq[Double](2, 4, 6)))
      }
    }
  }
}
