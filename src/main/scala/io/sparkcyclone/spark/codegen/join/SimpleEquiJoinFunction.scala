package io.sparkcyclone.spark.codegen.join

import io.sparkcyclone.spark.codegen.core.CFunction2.CFunctionArgument.{PointerPointer, Raw}
import io.sparkcyclone.spark.codegen.core.{CFunction2, CVector, CodeLines, VeType}

case class SimpleEquiJoinFunction(
  name: String,
  leftColumns: Seq[VeType],
  rightColunms: Seq[VeType]){
  val leftInputs: Seq[CVector] = leftColumns.zipWithIndex.map{ case (veType, idx) =>
    veType.makeCVector(s"left_input_$idx")
  }
  val rightInputs: Seq[CVector] = rightColunms.zipWithIndex.map{ case (veType, idx) =>
    veType.makeCVector(s"right_input_$idx")
  }
  val inputs: Seq[CVector] = leftInputs ++ rightInputs
  val leftOutputs: Seq[CVector] = leftColumns.zipWithIndex.map{ case (veType, idx) =>
    veType.makeCVector(s"left_out_$idx")
  }
  val rightOutputs: Seq[CVector] = rightColunms.tail.zipWithIndex.map{ case (veType, idx) =>
    veType.makeCVector(s"right_out_$idx")
  }
  val outputs: Seq[CVector] = leftOutputs ++ rightOutputs

  private val leftCountVarName = "leftBatchCount"
  private val rightCountVarName = "rightBatchCount"

  private val arguments = {
    List(
      Raw(s"size_t $leftCountVarName"),
      Raw(s"size_t $rightCountVarName"),
      // Those two parameters aren't used, but we provide them to match the
      // executeJoin calling convention
      Raw("size_t leftRows"),
      Raw("size_t rightRows"),
    ) ++ (
      inputs ++ outputs
      ).map(PointerPointer)
  }

  private val leftKey = leftInputs.head
  private val rightKey = rightInputs.head

  def body: CodeLines = CodeLines.from(
    mergeBatches(leftInputs, leftCountVarName),
    mergeBatches(rightInputs, rightCountVarName),
    outputs.map{ output =>
      val vectorType = output.veType.cVectorType
      s"*${output.name} = ${vectorType}::allocate();"
    },
    "std::vector<size_t> left_idxs;",
    "std::vector<size_t> right_idxs;",
    s"std::vector<size_t> left_keys = ${leftKey.name}_all->size_t_data_vec();",
    s"std::vector<size_t> right_keys = ${rightKey.name}_all->size_t_data_vec();",
    s"cyclone::equi_join_indices(left_keys, right_keys, left_idxs, right_idxs);",
    selectIntoOutputs(leftOutputs.zip(leftInputs), "left_idxs"),
    selectIntoOutputs(rightOutputs.zip(rightInputs.tail), "right_idxs"),
    inputs.map{ input =>
      s"free(${input.name}_all);"
    }
  )

  def toCFunction: CFunction2 = {
    CFunction2(name, arguments, body)
  }

  private def mergeBatches(columns: Seq[CVector], countVarName: String) = CodeLines.from(
    columns.map{ col =>
      val vectorType = col.veType.cVectorType
      s"$vectorType* ${col.name}_all = ${vectorType}::merge(${col.name}, $countVarName);"
    }
  )

  private def selectIntoOutputs(columns: Seq[(CVector, CVector)], indexName: String) = CodeLines.from(
    columns.map{ case (outCol, inCol) =>
      s"${outCol.name}[0] = ${inCol.name}_all->select($indexName);"
    }
  )
}
