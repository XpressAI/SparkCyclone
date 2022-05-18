package com.nec.ve.eval

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core._
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.nec.spark.agile.StringHole
import com.nec.spark.agile.StringHole.StringHoleEvaluation
import com.nec.spark.agile.StringHole.StringHoleEvaluation.LikeStringHoleEvaluation
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.CallContext
import com.nec.colvector.{VeColBatch, VeColVectorSource}
import com.nec.ve.eval.StaticTypingTestAdditions.{VeAllocator, VeRetriever}
import com.nec.ve.{VeKernelInfra, VeProcess, WithVeProcess}
import org.apache.arrow.memory.RootAllocator
import org.scalatest.Ignore
import org.scalatest.freespec.AnyFreeSpec

@Ignore
@VectorEngineTest
final class StringOpsStringHoleEvaluationSpec
  extends AnyFreeSpec
  with WithVeProcess
  with VeKernelInfra {

  import com.nec.util.CallContextOps._

  val list = List("this", "test", "is defi", "nitely", "tested")

  "It filters strings as expected for StartsWith" in {
    val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)

    expect(
      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
        input = list,
        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "test").startsWith
      ) == testedList
    )
  }

  "It filters strings as expected for EndsWith" in {
    val testedList = list.map(str => if (str.endsWith("d")) 1 else 0)

    expect(
      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
        input = list,
        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "d").endsWith
      ) == testedList
    )
  }

  "It filters strings as expected for Contains" in {
    val testedList = list.map(str => if (str.contains("s")) 1 else 0)

    expect(
      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
        input = list,
        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "s").contains
      ) == testedList
    )
  }

  "It filters strings as expected for Equals" in {
    val testedList = list.map(str => if (str == "test") 1 else 0)

    expect(
      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
        input = list,
        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "test").equalsTo
      ) == testedList
    )
  }

  "Fast evaluator filters strings as expected for StartsWith" in {
    val testedList = list.map(str => if (str.startsWith("test")) 1 else 0)

    expect(
      StringOpsStringHoleEvaluationSpec.executeHoleEvaluation(
        input = list,
        stringHoleEvaluation = LikeStringHoleEvaluation.Like("strings", "test").startsWith
      ) == testedList
    )
  }

}

object StringOpsStringHoleEvaluationSpec {
  def executeHoleEvaluation(input: List[String], stringHoleEvaluation: StringHoleEvaluation)(
    implicit
    veAllocator: VeAllocator[String],
    veRetriever: VeRetriever[Int],
    veProcess: VeProcess,
    veKernelInfra: VeKernelInfra,
    context: CallContext,
    veColVectorSource: VeColVectorSource
  ): Seq[Int] = {
    implicit val allocator = new RootAllocator(Integer.MAX_VALUE)

    val cFunction = CFunction(
      inputs = List(CVector.varChar("strings")),
      outputs = List(CVector.int("bools")),
      body = CodeLines.from(
        stringHoleEvaluation.computeVector,
        GroupByOutline
          .initializeScalarVector(VeNullableInt, "bools", "strings->count"),
        CodeLines.from(
          "for ( int i = 0; i < strings->count; i++ ) { ",
          GroupByOutline.storeTo("bools", stringHoleEvaluation.fetchResult, "i").indented,
          "}"
        ),
        "return 0;"
      )
    )

    veKernelInfra.compiledWithHeaders(cFunction, "test") { path =>
      val libRef = veProcess.loadLibrary(path)
      val inputVectors = veAllocator.allocate(input: _*)
      try {
        val resultingVectors =
          veProcess.execute(libRef, "test", inputVectors.columns.toList, veRetriever.makeCVectors)
        veRetriever.retrieve(VeColBatch(resultingVectors))
      } finally inputVectors.free()
    }
  }
}
