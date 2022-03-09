package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation._
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.planning.VeFunction
import com.nec.spark.planning.VeFunction.VeFunctionStatus

object SortFunction {
  final val SortedIndicesId = "sorted_indices"
}

case class SortFunction(
  name: String,
  data: List[CScalarVector],
  sorts: List[VeSortExpression]
) {
  require(data.nonEmpty, "Expected Sort to have at least one data column")
  require(sorts.nonEmpty, "Expected Sort to have at least one projection expression")

  lazy val inputs: List[CVector] = {
    data
  }

  lazy val outputs: List[CVector] = {
    data.map { case CScalarVector(name, vetype) =>
      CScalarVector(s"${name.replaceAllLiterally("input", "output")}", vetype)
    }
  }

  lazy val arguments: List[CFunction2.CFunctionArgument] = {
    inputs.map { vec => CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_m")) } ++
      outputs.map { vec => CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_mo")) }
  }

  private[ve] def inputPtrDeclStmts: CodeLines = {
    (data, inputs).zipped.map { case (dvec, ivec) =>
      s"const auto *${ivec.name} = ${ivec.name}_m[0];"
    }
  }

  private[ve] def outputPtrDeclStmts: CodeLines = {
    outputs.map { ovec =>
      CodeLines.from(
        s"auto *${ovec.name} = ${ovec.veType.cVectorType}::allocate();",
        s"*${ovec.name}_mo = ${ovec.name};"
      )
    }
  }

  def prepareColumnsStmts: CodeLines = {
    val hasNoNullCheck = sorts.exists(_.typedExpression.cExpression.isNotNullCode.isEmpty)
    CodeLines.from(
      // Set up a default vector of 1's if needed
      if (hasNoNullCheck) s"std::vector<int32_t> ONES(${inputs.head.name}->count, 1);" else "",
      sorts.zipWithIndex.map {
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

  def sortColumnsStmts: CodeLines = {
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

  def reorderStmts: CodeLines = {
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

  def toVeFunction: VeFunction = {
    VeFunction(
      VeFunctionStatus.fromCodeLines(toCFunction.toCodeLinesWithHeaders),
      name,
      outputs
    )
  }
}
