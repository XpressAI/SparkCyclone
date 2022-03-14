package com.nec.spark.agile.sort

import com.nec.spark.agile.core.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.core.{CFunction2, FunctionTemplateTrait}

object SortFunction {
  final val SortedIndicesId = "sorted_indices"
}

case class SortFunction(
  name: String,
  data: List[CScalarVector],
  sorts: List[VeSortExpression]
) extends FunctionTemplateTrait {
  require(data.nonEmpty, "Expected Sort to have at least one data column")
  require(sorts.nonEmpty, "Expected Sort to have at least one projection expression")

  private[sort] lazy val inputs: List[CVector] = {
    data
  }

  lazy val outputs: List[CVector] = {
    data.map { case CScalarVector(name, vetype) =>
      CScalarVector(s"${name.replaceAllLiterally("input", "output")}", vetype)
    }
  }

  private[sort] lazy val arguments: List[CFunction2.CFunctionArgument] = {
    inputs.map { vec => CFunction2.CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_m")) } ++
      outputs.map { vec => CFunction2.CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_mo")) }
  }

  private[sort] def inputPtrDeclStmts: CodeLines = {
    inputs.map { input =>
      s"const auto *${input.name} = ${input.name}_m[0];"
    }
  }

  private[sort] def outputPtrDeclStmts: CodeLines = {
    outputs.map { output =>
      CodeLines.from(
        s"auto *${output.name} = ${output.veType.cVectorType}::allocate();",
        s"*${output.name}_mo = ${output.name};"
      )
    }
  }

  private[sort] def prepareColumnsStmts: CodeLines = {
    val hasNoNullCheck = sorts.exists(_.typedExpression.cExpression.isNotNullCode.isEmpty)
    CodeLines.from(
      // Set up a default vector of 1's if needed
      if (hasNoNullCheck) s"std::vector<int32_t> ONES(${inputs.head.name}->count, 1);" else "",
      sorts.zipWithIndex.map {
        /*
          Since expressions and notNullCodes will be of the form `input_x->data[i]`
          and `input_x->get_validity(i)`, a simple string replacement is performed
          to generate the right code.  This should be replaced by something more
          typesafe in the future.
        */
        case (VeSortExpression(TypedCExpression2(_, CExpression(cCode, Some(notNullCode))), _), idx) =>
          CodeLines.from(
            // Extract the pointer to the data array
            s"auto *tmp${idx}a = ${cCode.replaceAll("""\[.*\]""", "")};",
            // Extract the pointer to the validity vec
            s"auto tmp${idx}b0 = ${notNullCode.replaceAll("->get_validity(.*)", "")}->validity_vec();",
            s"auto *tmp${idx}b = tmp${idx}b0.data();",
          )

        case (VeSortExpression(TypedCExpression2(_, CExpression(cCode, None)), _), idx) =>
          CodeLines.from(
            // Extract the pointer to the data array
            s"auto *tmp${idx}a = ${cCode.replaceAll("""\[.*\]""", "")};",
            // Extract the pointer to the default vector of 1's
            s"auto *tmp${idx}b = ONES.data();"
          )
      }
    )
  }

  private[sort] def sortColumnsStmts: CodeLines = {
    val arguments = sorts.zipWithIndex.flatMap { case (expr, idx) =>
      val ordering = expr.sortOrdering match {
        case Ascending  => 1
        case Descending => 0
      }

      List(
        s"std::make_tuple(${ordering}, tmp${idx}a)",
        s"std::make_tuple(${ordering}, tmp${idx}b)"
      )
    }

    s"const auto ${SortFunction.SortedIndicesId} = cyclone::sort_columns(${inputs.head.name}->count, ${arguments.mkString(", ")});"
  }

  private[sort] def reorderStmts: CodeLines = {
    (outputs, inputs).zipped.map { case (output, input) =>
      s"${output.name}->move_assign_from(${input.name}->select(${SortFunction.SortedIndicesId}));"
    }
  }

  def toCFunction: CFunction2 = {
    CFunction2(
      name,
      arguments,
      CodeLines.from(inputPtrDeclStmts, "", outputPtrDeclStmts, "", prepareColumnsStmts, "", sortColumnsStmts, "", reorderStmts)
    )
  }
}
