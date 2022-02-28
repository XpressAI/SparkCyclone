package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{VeFilter, CExpression, CScalarVector, CVarChar, CVector}
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunction2.CFunctionArgument.{Pointer, PointerPointer}

object FilterFunction {
  val BitMaskId = "mask"
  val MatchListId = "matching_ids"
}

case class FilterFunction(name: String,
                          filter: VeFilter[CVector, CExpression],
                          onVe: Boolean = true) {
  require(filter.data.nonEmpty, "Expected Filter to have at least one data column")

  lazy val inputs: List[CVector] = {
    filter.data.map { vec => vec.withNewName(s"${vec.name}_m") }
  }

  lazy val outputs: List[CVector] = {
    filter.data.map { vec => vec.withNewName(s"${vec.name.replaceAllLiterally("input", "output")}_m") }
  }

  lazy val arguments: List[CFunction2.CFunctionArgument] = {
    if (onVe) {
      inputs.map(PointerPointer(_)) ++ outputs.map(PointerPointer(_))
    } else {
      inputs.map(Pointer(_)) ++ outputs.map(Pointer(_))
    }
  }

  private[ve] def filterVecStmt(input: CVector): CodeLines = {
    val output = input.replaceName("input", "output")
    CodeLines.scoped(s"Populate ${output.name}[0] based on the filter applied to ${input.name}[0]") {
      if (onVe) {
        // Create a filtered copy of input and assign the pointer to output*
        s"*${output.name} = ${input.name}[0]->filter(${FilterFunction.MatchListId});",
      } else {
        // A memory region is already provided, so create a filtered copy of input and move to the region
        s"${output.name}->move_assign_from(${input.name}->filter(${FilterFunction.MatchListId}));"
      }
    }
  }

  private[ve] def computeFilterStmt: CodeLines = {
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
              s"std::vector<size_t> ${FilterFunction.BitMaskId}(${filter.data.head.name}->count);",
              CodeLines.forLoop("i", s"${filter.data.head.name}->count") {
                s"${FilterFunction.BitMaskId}[i] = ${filterCondition};"
              },
              "",
              "// Compute the matching_ids from the bitmask",
              s"matching_ids = cyclone::bitmask_to_matching_ids(${FilterFunction.BitMaskId});",
            )
          }
        )
      }
    )
  }

  private[ve] def ptrDeclStmts: CodeLines = {
    (filter.data, inputs).zipped.map { case (dvec, ivec) =>
      if (onVe) {
        s"${dvec.declarePointer} = ${ivec.name}[0];"
      } else {
        s"${dvec.declarePointer} = ${ivec.name};"
      }
    }
  }

  def render: CFunction2 = {
    val body = CodeLines.from(
      // Declare some pointers
      ptrDeclStmts,
      "",
      // Perform the filter
      computeFilterStmt,
      // Copy elements over to the output based on the matching_ids
      inputs.map(filterVecStmt)
    )

    CFunction2(arguments, body)
  }

  def toCodeLines: CodeLines = {
    render.toCodeLines(name)
  }
}
