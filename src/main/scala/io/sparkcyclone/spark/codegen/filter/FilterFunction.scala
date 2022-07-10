package io.sparkcyclone.spark.codegen.filter

import io.sparkcyclone.spark.codegen.VeFunctionTemplate
import io.sparkcyclone.spark.codegen.CFunctionGeneration.CExpression
import io.sparkcyclone.spark.codegen.StringHole.StringHoleEvaluation
import io.sparkcyclone.native.code.CFunction2.CFunctionArgument.{Pointer, PointerPointer}
import io.sparkcyclone.native.code._

final case class VeFilter[Data, Condition](
  stringVectorComputations: Seq[StringHoleEvaluation],
  data: Seq[Data],
  condition: Condition
)

object FilterFunction {
  final val BitMaskId = "mask"
  final val MatchListId = "matching_ids"
}

final case class FilterFunction(name: String,
                                filter: VeFilter[CVector, CExpression],
                                onVe: Boolean = true) extends VeFunctionTemplate {
  require(filter.data.nonEmpty, "Expected Filter to have at least one data column")

  private[filter] lazy val inputs: Seq[CVector] = {
    filter.data.map { vec => vec.withNewName(s"${vec.name}_m") }
  }

  lazy val outputs: Seq[CVector] = {
    filter.data.map { vec =>
      vec.withNewName(s"${vec.name.replaceAllLiterally("input", "output")}_m")
    }
  }

  private[filter] lazy val arguments: Seq[CFunction2.CFunctionArgument] = {
    if (onVe) {
      inputs.map(PointerPointer(_)) ++ outputs.map(PointerPointer(_))
    } else {
      inputs.map(Pointer(_)) ++ outputs.map(Pointer(_))
    }
  }

  private[filter] def applySelectionStmt(input: CVector): CodeLines = {
    val output = input.replaceName("input", "output")
    CodeLines.scoped(
      s"Populate ${output.name}[0] based on the filter applied to ${input.name}[0]"
    ) {
      if (onVe) {
        // Create a filtered copy of input and assign the pointer to output*
        s"*${output.name} = ${input.name}[0]->select(${FilterFunction.MatchListId});"
      } else {
        // A memory region is already provided, so create a filtered copy of input and move to the region
        s"${output.name}->move_assign_from(${input.name}->select(${FilterFunction.MatchListId}));"
      }
    }
  }

  private[filter] def computeFilterStmt: CodeLines = {
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
              s"matching_ids = cyclone::bitmask_to_matching_ids(${FilterFunction.BitMaskId});"
            )
          }
        )
      }
    )
  }

  private[filter] def inputPtrDeclStmts: CodeLines = {
    (filter.data, inputs).zipped.map { case (dvec, ivec) =>
      if (onVe) {
        s"${dvec.declarePointer} = ${ivec.name}[0];"
      } else {
        s"${dvec.declarePointer} = ${ivec.name};"
      }
    }
  }

  def hashId: Int = {
    /*
      The semantic identity of the FilterFunction will be determined by the
      filter expression and whether or not it is intended to run on VE.
    */
    (getClass.getName, filter, onVe).hashCode
  }

  def toCFunction: CFunction2 = {
    val body = CodeLines.from(
      // Declare some pointers
      inputPtrDeclStmts,
      "",
      // Perform the filter
      computeFilterStmt,
      // Copy elements over to the output based on the matching_ids
      inputs.map(applySelectionStmt)
    )

    CFunction2(name, arguments, body)
  }

  def secondary: Seq[CFunction2] = {
    Seq.empty
  }
}
