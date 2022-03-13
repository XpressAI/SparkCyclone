package com.nec.spark.agile.merge

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeType}

case class MergeFunction(name: String,
                         columns: List[VeType]) {
  require(columns.nonEmpty, "Expected Merge to have at least one data column")

  lazy val inputs: List[CVector] = {
    columns.zipWithIndex.map { case (veType, idx) =>
      veType.makeCVector(s"input_${idx}_g")
    }
  }

  lazy val outputs: List[CVector] = {
    columns.zipWithIndex.map { case (veType, idx) =>
      veType.makeCVector(s"output_${idx}_g")
    }
  }

  lazy val arguments: List[CFunction2.CFunctionArgument] = {
    List(CFunctionArgument.Raw("int batches"), CFunctionArgument.Raw("int rows")) ++
      inputs.map(CFunctionArgument.PointerPointer(_)) ++
      outputs.map(CFunctionArgument.PointerPointer(_))
  }

  def mergeCVecStmt(vetype: VeType, index: Int): CodeLines = {
    val in = s"input_${index}_g"
    val out = s"output_${index}_g"

    CodeLines.scoped(s"Merge ${in}[...] into ${out}[0]") {
      CodeLines.from(
        /*
          Allocate the nullable_T_vector[] with size buckets

          NOTE: This cast is incorrect, because we are allocating a T* array
          (T**) but type-casting it to T*.  However, for some reason, fixing
          this will lead an invalid free() later on - this is likely due to an
          error in how we define function call from the Spark side.  Will need
          to investigate and fix this in the future.
        */
        s"// Allocate T*[] but cast to T* (incorrect but required to work correctly until a fix lands)",
        s"*${out} = static_cast<${vetype.cVectorType}*>(malloc(sizeof(nullptr)));",
        // Merge inputs and assign output to pointer
        s"${out}[0] = ${vetype.cVectorType}::merge(${in}, batches);",
      )
    }
  }

  def render: CFunction2 = {
    CFunction2(name, arguments, columns.zipWithIndex.map((mergeCVecStmt _).tupled))
  }

  def toCodeLines: CodeLines = {
    render.toCodeLines
  }
}
