package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeType}

object GroupingFunction {
  val GroupAssignmentsId = "bucket_assignments"
  val GroupCountsId = "bucket_counts"

  sealed trait KeyOrValue {
    def render: String
  }

  case object Key extends KeyOrValue {
    def render: String = "key"
  }

  case object Value extends KeyOrValue {
    def render: String = "value"
  }

  final case class DataDescription(veType: VeType, kvType: KeyOrValue)
}

case class GroupingFunction(name: String,
                            columns: List[GroupingFunction.DataDescription],
                            nbuckets: Int) {
  require(columns.nonEmpty, "Expected Grouping to have at least one data column")

  lazy val inputs: List[CVector] = {
    columns.zipWithIndex.map { case (GroupingFunction.DataDescription(veType, kvType), idx) =>
      veType.makeCVector(s"${kvType.render}_${idx}")
    }
  }

  lazy val outputs: List[CVector] = {
    columns.zipWithIndex.map { case (GroupingFunction.DataDescription(veType, kvType), idx) =>
      veType.makeCVector(s"output_${kvType.render}_${idx}")
    }
  }

  private[ve] lazy val keycols = columns.zip(inputs).filter(_._1.kvType == GroupingFunction.Key).map(_._2)

  lazy val arguments: List[CFunction2.CFunctionArgument] = {
    inputs.map(PointerPointer(_)) ++
      List(CFunctionArgument.Raw("int* sets")) ++
      outputs.map(PointerPointer(_))
  }

  private[ve] def computeBucketAssignments: CodeLines = {
    CodeLines.from(
      // Initialize the bucket_assignments table
      s"std::vector<size_t> ${GroupingFunction.GroupAssignmentsId}(${keycols.head.name}[0]->count);",
      CodeLines.scoped("Compute the index -> bucket mapping") {
        CodeLines.from(
          "#pragma _NEC vector",
          CodeLines.forLoop("i", s"${keycols.head.name}[0]->count") {
            CodeLines.from(
              // Initialize the hash
              s"int64_t hash = 1;",
              // Compute the hash across all keys
              keycols.map { vec => s"hash = ${vec.name}[0]->hash_at(i, hash);" },
              // Assign the bucket based on the hash
              s"${GroupingFunction.GroupAssignmentsId}[i] = __builtin_abs(hash % ${nbuckets});"
            )
          }
        )
      }
    )
  }

  private[ve] def computeBucketCounts: CodeLines = {
    CodeLines.from(
      // Iniitalize the bucket_counts table
      s"std::vector<size_t> ${GroupingFunction.GroupCountsId}(${nbuckets});",
      CodeLines.scoped("Compute the value counts for each bucket") {
        CodeLines.from(
          "#pragma _NEC vector",
          CodeLines.forLoop("g", s"${nbuckets}") {
            CodeLines.from(
              s"size_t count = 0;",
              // Count the assignments that equal g
              CodeLines.forLoop("i", s"${GroupingFunction.GroupAssignmentsId}.size()") {
                s"count += (${GroupingFunction.GroupAssignmentsId}[i] == g);"
              },
              // Assign to the counts table
              s"${GroupingFunction.GroupCountsId}[g] = count;"
            )
          }
        )
      }
    )
  }

  private[ve] def cloneCVecStmt(output: CVector, input: CVector): CodeLines = {
    CodeLines.scoped(s"Clone ${input.name}[0] over to ${output.name}[0]") {
      List(
        // Allocate the nullable_T_vector[] with size 1
        s"*${output.name} = static_cast<${output.veType.cVectorType} *>(malloc(sizeof(nullptr)));",
        // Clone the input nullable_T_vector to output nullable_T_vector at [0]
        s"${output.name}[0] = ${input.name}[0]->clone();",
      )
    }
  }

  private[ve] def copyVecToBucketsStmt(output: CVector, input: CVector): CodeLines = {
    CodeLines.scoped(
      s"Copy elements of ${input.name}[0] to their respective buckets in ${output.name}"
    ) {
      CodeLines.from(
        // Perform the bucketing and get back T** (array of pointers)
        s"auto ** tmp = ${input.name}[0]->bucket(${GroupingFunction.GroupCountsId}, ${GroupingFunction.GroupAssignmentsId});",
        /*
          Allocate the nullable_T_vector[] with size buckets

          NOTE: This cast is incorrect, because we are allocating a T* array
          (T**) but type-casting it to T*.  However, for some reason, fixing
          this will lead an invalid free() later on - this is likely due to an
          error in how we define function call from the Spark side.  Will need
          to investigate and fix this in the future.
        */
        s"// Allocate T*[] but cast to T* (incorrect but required to work correctly until a fix lands)",
        s"*${output.name} = static_cast<${output.veType.cVectorType} *>(malloc(sizeof(nullptr) * ${nbuckets}));",
        // Copy the pointers over
        CodeLines.forLoop("b", s"${nbuckets}") {
          s"${output.name}[b] = tmp[b];"
        },
        // Free the array of temporary pointers (but not the structs themselves)
        "free(tmp);"
      )
    }
  }

  def render: CFunction2 = {
    val body = if (keycols.isEmpty) {
      CodeLines.from(
        "// Write out the number of buckets",
        s"sets[0] = 1;",
        "",
        (outputs, inputs).zipped.map(cloneCVecStmt(_, _))
      )

    } else {
      CodeLines.from(
        computeBucketAssignments,
        computeBucketCounts,
        "// Write out the number of buckets",
        s"sets[0] = ${nbuckets};",
        "",
        (outputs, inputs).zipped.map(copyVecToBucketsStmt(_, _))
      )
    }

    CFunction2(arguments, body)
  }

  def toCodeLines: CodeLines = {
    render.toCodeLines(name)
  }
}
