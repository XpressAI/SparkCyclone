package com.nec.cmake.functions

import com.nec.arrow.ArrowNativeInterface.SupportedVectorWrapper
import com.nec.arrow.TransferDefinitions.TransferDefinitionsSourceCode
import com.nec.arrow.{ArrowVectorBuilders, CArrowNativeInterface, WithTestAllocator}
import com.nec.cmake.CMakeBuilder
import com.nec.util.RichVectors._
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CFunction, CVector}
import com.nec.spark.agile.StringProducer.FrovedisCopyStringProducer
import org.scalacheck.{Gen, Prop}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.Checkers

final class WordsCheckSpec extends AnyFreeSpec with Checkers {
  "it works" in {
    val someString: Gen[String] = Gen.asciiStr
    val listOfStr = Gen.listOf(someString)

    val cLib = CMakeBuilder.buildCLogging(
      List(
        TransferDefinitionsSourceCode,
        "\n\n",
        CFunction(
          inputs = List(CVector.varChar("input_0")),
          outputs = List(CVector.varChar("output_0")),
          body = CodeLines.from(
            "frovedis::words w = varchar_vector_to_words(input_0);",
            "words_to_varchar_vector(w, output_0);",
            "return 0;"
          )
        ).toCodeLinesS("test").cCode
      )
        .mkString("\n\n")
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val p: Prop = Prop.forAll(listOfStr)(list => {
        ArrowVectorBuilders.withArrowStringVector(list) { inVec =>
          ArrowVectorBuilders.withArrowStringVector(Seq.empty) { outVec =>
            nativeInterface.callFunction(
              name = "test",
              inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
              outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
            )
            outVec.toList == list
          }
        }
      })
      check(p)
    }
  }

  "we can produce a subset of strings" in {
    val someString: Gen[String] = Gen.asciiStr
    val listOfStr = Gen.listOf(someString)

    val prod = FrovedisCopyStringProducer("input_0")
    val cLib = CMakeBuilder.buildCLogging(
      cSource = List(
        TransferDefinitionsSourceCode,
        "\n\n",
        CFunction(
          inputs = List(CVector.varChar("input_0")),
          outputs = List(CVector.varChar("output_0")),
          body = CodeLines.from(
            CodeLines.debugHere,
            s"int size = input_0->count % 2 == 0 ? (input_0->count) / 2 : ((1 + input_0->count) / 2);",
            CodeLines.debugValue(""""input_0->count"""", "input_0->count"),
            CodeLines.debugValue(""""size"""", "size"),
            prod.init("output_0", "size"),
            CodeLines.debugHere,
            "int g = 0;",
            "for(int i = 0; i < input_0->count; i++) {",
            CodeLines
              .from(
                CodeLines.debugValue(""""i"""", "i"),
                CodeLines.debugHere,
                s"if ( g < size && i < input_0->count ) {",
                CodeLines.debugValue(""""sz"""", "output_0_input_words.starts.size()"),
                CodeLines.debugValue(""""lsz"""", "output_0_input_words.lens.size()"),
                CodeLines
                  .from(
                    CodeLines.debugValue("g"),
                    CodeLines.debugHere,
                    prod.produce("output_0", "g")
                  )
                  .indented,
                "}",
                CodeLines.debugHere,
                "i++;",
                "g++;",
                CodeLines.debugHere
              )
              .indented,
            "}",
            CodeLines.debugHere,
            prod.complete("output_0"),
            CodeLines.debugHere,
            "return 0;"
          )
        ).toCodeLinesS("test").cCode
      )
        .mkString("\n\n"),
      debug = true
    )

    val nativeInterface = new CArrowNativeInterface(cLib.toString)
    WithTestAllocator { implicit allocator =>
      val p: Prop = Prop.forAll(listOfStr)(list => {
        val expected = list.zipWithIndex.collect { case (s, idx) if idx % 2 == 0 => s }.toList
        val r = ArrowVectorBuilders.withArrowStringVector(list) { inVec =>
          ArrowVectorBuilders.withArrowStringVector(Seq.empty) { outVec =>
            nativeInterface.callFunction(
              name = "test",
              inputArguments = List(Some(SupportedVectorWrapper.wrapInput(inVec)), None),
              outputArguments = List(None, Some(SupportedVectorWrapper.wrapOutput(outVec)))
            )
            outVec.toList
          }
        }

        if (r != expected)
          info(
            s"result => ${r}; expected ${expected} (${r.map(_.length)}; ${expected.map(_.length)})"
          )

        r == expected
      })
      check(p)
    }
  }
}
