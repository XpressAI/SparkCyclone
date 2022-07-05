package io.sparkcyclone.spark.agile.sort

import io.sparkcyclone.spark.agile.core._
import io.sparkcyclone.spark.agile.CFunctionGeneration._
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, NullsFirst, NullsLast, NullOrdering, SortDirection}

object SortFunction {
  final val SortedIndicesId = "sorted_indices"
}

final case class SortFunction(name: String,
                              data: Seq[CScalarVector],
                              sorts: Seq[VeSortExpression]) extends VeFunctionTemplate {
  require(data.nonEmpty, "Expected Sort to have at least one data column")
  require(sorts.nonEmpty, "Expected Sort to have at least one projection expression")

  private[sort] lazy val inputs: Seq[CVector] = {
    data
  }

  lazy val outputs: Seq[CVector] = {
    data.map { case CScalarVector(name, vetype) =>
      CScalarVector(s"${name.replaceAllLiterally("input", "output")}", vetype)
    }
  }

  private[sort] lazy val arguments: Seq[CFunction2.CFunctionArgument] = {
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
    val hasNoNullCheck = sorts.exists(_.expression.cExpression.isNotNullCode.isEmpty)
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
        case (VeSortExpression(TypedCExpression2(_, CExpression(cCode, Some(notNullCode))), _, _), idx) =>
          CodeLines.from(
            // Extract the pointer to the data array
            s"auto *tmp${idx}a = ${cCode.replaceAll("""\[.*\]""", "")};",
            // Extract the pointer to the validity vec
            s"auto tmp${idx}b0 = ${notNullCode.replaceAll("->get_validity(.*)", "")}->validity_vec();",
            s"auto *tmp${idx}b = tmp${idx}b0.data();",
          )

        case (VeSortExpression(TypedCExpression2(_, CExpression(cCode, None)), _, _), idx) =>
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
    val arguments = sorts.zipWithIndex.flatMap { case (sort, idx) =>
      val ordering1 = sort.direction match {
        case Ascending  => 1
        case Descending => 0
      }

      val ordering2 = sort.nullOrdering match {
        case NullsFirst => 1
        case NullsLast  => 0
      }

      List(
        // Sort by the values
        s"std::make_tuple(${ordering1}, tmp${idx}a)",
        // Sort by the validity
        s"std::make_tuple(${ordering2}, tmp${idx}b)"
      )
    }

    s"const auto ${SortFunction.SortedIndicesId} = cyclone::sort_columns(${inputs.head.name}->count, ${arguments.mkString(", ")});"
  }

  private[sort] def reorderStmts: CodeLines = {
    (outputs, inputs).zipped.map { case (output, input) =>
      s"${output.name}->move_assign_from(${input.name}->select(${SortFunction.SortedIndicesId}));"
    }
  }

  def hashId: Int = {
    /*
      The semantic identity of the SortFunction will be determined by the
      data columns and sort expressions.
    */
    (getClass.getName, data.map(_.veType), sorts).hashCode
  }

  def toCFunction: CFunction2 = {
    CFunction2(
      name,
      arguments,
      CodeLines.from(inputPtrDeclStmts, "", outputPtrDeclStmts, "", prepareColumnsStmts, "", sortColumnsStmts, "", reorderStmts)
    )
  }

  def secondary: Seq[CFunction2] = {
    Seq.empty
  }
}
