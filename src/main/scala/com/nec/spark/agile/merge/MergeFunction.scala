package com.nec.spark.agile.merge

import com.nec.spark.agile.core.CFunction2.CFunctionArgument
import com.nec.spark.agile.core._

case class MergeFunction(name: String,
                         columns: Seq[VeType]) extends VeFunctionTemplate {
  require(columns.nonEmpty, "Expected Merge to have at least one data column")

  private[merge] lazy val inputs: List[CVector] = {
    columns.toList.zipWithIndex.map { case (veType, idx) =>
      veType.makeCVector(s"input_${idx}_g")
    }
  }

  lazy val outputs: Seq[CVector] = {
    columns.toList.zipWithIndex.map { case (veType, idx) =>
      veType.makeCVector(s"output_${idx}_g")
    }
  }

  private[merge] lazy val arguments: List[CFunction2.CFunctionArgument] = {
    List(CFunctionArgument.Raw("int batches"), CFunctionArgument.Raw("int rows")) ++
      inputs.map(CFunctionArgument.PointerPointer(_)) ++
      outputs.map(CFunctionArgument.PointerPointer(_))
  }

  private[merge] def mergeCVecStmt(vetype: VeType, index: Int): CodeLines = {
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
        //s"""std::cout << "${in} address" << ${in} << std::endl;""",
        s"// Allocate T*[] but cast to T* (incorrect but required to work correctly until a fix lands)",
        s"*${out} = static_cast<${vetype.cVectorType}*>(malloc(sizeof(nullptr)));",
        //s"""std::cout << "${out} address" << ${out} << std::endl;""",
        //s"""std::cout << "batches: " << batches << std::endl;""",
        // Merge inputs and assign output to pointer
        s"${out}[0] = ${vetype.cVectorType}::merge(${in}, batches);",
      )
    }
  }

  def hashId: Int = {
    /*
      The semantic identity of the MergeFunction will be determined by the
      grouping columns and number of buckets.
    */
    (getClass.getName, columns).hashCode
  }

  def toCFunction: CFunction2 = {
    CFunction2(name, arguments, columns.zipWithIndex.map((mergeCVecStmt _).tupled))
  }

  def secondary: Seq[CFunction2] = {
    Seq.empty
  }
}
