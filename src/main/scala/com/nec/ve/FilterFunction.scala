package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{VeFilter, CExpression, CScalarVector, CVarChar, CVector}
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunction2.CFunctionArgument.PointerPointer

object FilterFunction {
  val MatchListId = "matching_ids"
}

case class FilterFunction(name: String,
                          filter: VeFilter[CVector, CExpression]) {
  require(filter.data.nonEmpty, "Expected Filter to have at least one data column")

  lazy val inputs: List[CVector] = {
    filter.data.map { vec => vec.withNewName(s"${vec.name}_m") }
  }

  lazy val outputs: List[CVector] = {
    filter.data.map { vec => vec.withNewName(s"${vec.name.replaceAllLiterally("input", "output")}_m") }
  }

  lazy val arguments: List[CFunction2.CFunctionArgument] = {
    inputs.map(PointerPointer(_)) ++ outputs.map(PointerPointer(_))
  }

  def filterVecStmt(input: CVector): CodeLines = {
    val output = input.replaceName("input", "output")
    CodeLines.scoped(s"Populate ${output.name}[0] based on the filter applied to ${input.name}[0]") {
      // Created a filtered copy of input and assign the pointer to output*
      s"*${output.name} = ${input.name}[0]->filter(${FilterFunction.MatchListId});",
    }
  }

  def computeFilterStmt: CodeLines = {
    // Final filter condition that is the AND of individual C expressions and output of *Hole evaluations
    val filterCondition = filter.condition.isNotNullCode match {
      case Some(x) =>
        s"${x} && ${filter.condition.cCode}"
      case None =>
        filter.condition.cCode
    }

    CodeLines.from(
      s"std::vector<size_t> matching_ids;",
      CodeLines.scoped("Perform the filter operation") {
        CodeLines.from(
          // Execute *Hole evaluations
          filter.stringVectorComputations.distinct.map(_.computeVector),
          CodeLines.scoped("Combined the sub-filter results to matching_ids") {
            CodeLines.from(
              // Generate mask array
              "// Combine all filters to a mask array",
              s"std::vector<size_t> mask(${inputs.head.name}[0]->count);",
              CodeLines.forLoop("i", s"${inputs.head.name}[0]->count") {
                s"mask[i] = ${filterCondition};"
              },
              "",
              "// Count the bits with value of 1",
              "size_t m_count = 0;",
              CodeLines.forLoop("i", "mask.size()") {
                "m_count += mask[i];"
              },
              "",
              "// Add the indices of the 1's to the matching_ids",
              "matching_ids.resize(m_count);",
              "size_t mz = 0;",
              // Add #pragma to guide vectorization
              "#pragma _NEC vector",
              CodeLines.forLoop("i", "mask.size()") {
                CodeLines.ifStatement("mask[i]") {
                  "matching_ids[mz++] = i;"
                }
              }
            )
          }
        )
      }
    )
  }

  def render: CFunction2 = {
    val body = CodeLines.from(
      // Declare some pointers
      (filter.data, inputs).zipped.map { case (dvec, ivec) =>
        s"${dvec.declarePointer} = ${ivec.name}[0];"
      },
      "",
      // Perform the filter
      computeFilterStmt,
      // Deallocate data used for the string vector computations
      filter.stringVectorComputations.distinct.map(_.deallocData),
      // Copy elements over to the output based on the matching_ids
      inputs.map(filterVecStmt)
    )

    CFunction2(arguments, body)
  }

  def toCodeLines: CodeLines = {
    render.toCodeLines(name)
  }
}
