package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeScalarType, VeType}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.spark.agile.{CFunction2, CFunctionGeneration, StringProducer}
import com.nec.ve.GroupingFunction.DataDescription.KeyOrValue

object GroupingFunction {

  def addStringHashing(source: String, index: String, toHash: String): CodeLines = {
    val stringStart = s"$source->offsets[$index]"
    val stringLength = s"$source->offsets[$index + 1] - $stringStart"
    CodeLines.from(CodeLines.forLoop("x", stringLength) {
      CodeLines.from(s"hash = 31 * hash + ${source}->data[x + $stringStart];")
    })
  }

  final case class DataDescription(veType: VeType, keyOrValue: KeyOrValue)

  object DataDescription {
    sealed trait KeyOrValue {
      def renderValue: String
      def isKey: Boolean
      def isValue: Boolean = !isKey
    }
    object KeyOrValue {
      case object Key extends KeyOrValue {
        override def renderValue: String = "key"
        override def isKey: Boolean = true
      }
      case object Value extends KeyOrValue {
        override def renderValue: String = "value"
        override def isKey: Boolean = false
      }
    }
  }

  def computeBuckets(
    cVectors: List[CVector],
    groupingIdentifiers: String,
    totalBuckets: Int
  ): CodeLines = {
    require(cVectors.nonEmpty, "cVectors is empty - perhaps an issue in checking the groups?")
    CodeLines.from(
      s"// Compute the index -> bucket mapping",
      s"std::vector<size_t> $groupingIdentifiers;",
      CodeLines.forLoop("i", s"${cVectors.head.name}[0]->count") {
        CodeLines.from(
          s"int hash = 1;",
          cVectors.map { cVector =>
            CodeLines.from(
              if (cVector.veType.isString) addStringHashing(s"${cVector.name}[0]", "i", "hash")
              else CodeLines.from(s"hash = 31 * ${cVector.name}[0]->data[i];")
            )
          },
          s"$groupingIdentifiers.push_back(abs(hash % ${totalBuckets}));"
        )
      }
    )
  }

  def computeBucketSizes(
    groupingIdentifiers: String,
    bucketToCount: String,
    totalBuckets: Int
  ): CodeLines = {
    CodeLines.from(
      s"// Compute the value counts for each bucket",
      s"std::vector<size_t> $bucketToCount;",
      CodeLines.forLoop("g", s"$totalBuckets") {
        CodeLines.from(
          s"int cnt = 0;",
          CodeLines.forLoop("i", s"${groupingIdentifiers}.size()") {
            CodeLines.ifStatement(s"${groupingIdentifiers}[i] == g") {
              "cnt++;"
            }
          },
          s"$bucketToCount.push_back(cnt);"
        )
      }
    )
  }

  def cloneCVecStmt(output: CVector, input: CVector): CodeLines = {
    require(
      output.veType == input.veType,
      "Cannot clone cVector - input and output VeTypes are not the same!"
    )

    CodeLines.scoped(s"Clone ${input.name}[0] over to ${output.name}[0]") {
      List(
        // Allocate the nullable_T_vector[] with size 1
        s"*${output.name} = static_cast<${output.veType.cVectorType} *>(malloc(sizeof(nullptr)));",
        // Clone the input nullable_T_vector to output nullable_T_vector at [0]
        s"${output.name}[0] = ${input.name}[0]->clone();",
      )
    }
  }

  def groupData(data: List[DataDescription], totalBuckets: Int) = {
    if (data.count(_.keyOrValue.isKey) <= 0) {
      /*
        In the case where there are no input key columns, we simply clone the
        input data to the output pointers.
       */
      groupDataNoKeyColumns(data)

    } else {
      groupDataNormal(data, totalBuckets)
    }
  }

  def groupDataNoKeyColumns(data: List[DataDescription]): CFunction2 = {
    val inputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"${isKey.renderValue}_${idx}")
    }

    val outputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"output_${isKey.renderValue}_${idx}")
    }

    val arguments = inputs.map(PointerPointer(_)) ++
      List(CFunctionArgument.Raw("int* sets")) ++
      outputs.map(PointerPointer(_))

    CFunction2(
      arguments,
      CodeLines.from(
        "// Write out the number of buckets",
        s"sets[0] = 1;",
        "",
        (outputs, inputs).zipped.map(cloneCVecStmt(_, _))
      )
    )
  }

  def copyVecToBucketsStmt(
    output: CVector,
    input: CVector,
    buckets: Int,
    idToBucket: String,
    bucketToCount: String
  ): CodeLines = {
    require(
      output.veType == input.veType,
      "Cannot copy cVector values to their buckets - input and output VeTypes are not the same!"
    )

    CodeLines.scoped(
      s"Copy elements of ${input.name}[0] to their respective buckets in ${output.name}"
    ) {
      CodeLines.from(
        // Perform the bucketing and get back T** (array of pointers)
        s"auto ** tmp = ${input.name}[0]->bucket(${bucketToCount}, ${idToBucket});",
        /*
          Allocate the nullable_T_vector[] with size buckets

          NOTE: This cast is incorrect, because we are allocating a T* array
          (T**) but type-casting it to T*.  However, for some reason, fixing
          this will lead an invalid free() later on.  Will need to investigate
          and fix this in the future.
        */
        s"// Allocate T*[] but cast to T* (incorrect but required to work correctly until a fix lands)",
        s"*${output.name} = static_cast<${output.veType.cVectorType} *>(malloc(sizeof(nullptr) * ${buckets}));",
        // Copy the pointers over
        CodeLines.forLoop("b", s"${buckets}") {
          s"${output.name}[b] = tmp[b];"
        },
        // Free the array of temporary pointers (but not the structs themselves)
        "free(tmp);"
      )
    }
  }

  def groupDataNormal(data: List[DataDescription], totalBuckets: Int): CFunction2 = {
    val inputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"${isKey.renderValue}_$idx")
    }

    val outputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"output_${isKey.renderValue}_$idx")
    }

    val arguments = inputs.map(PointerPointer(_)) ++
      List(CFunctionArgument.Raw("int* sets")) ++
      outputs.map(PointerPointer(_))

    CFunction2(
      arguments = arguments,
      body = CodeLines.from(
        // Compute the bucket assignments
        computeBuckets(
          cVectors = data.zip(inputs).filter(_._1.keyOrValue.isKey).map(_._2),
          groupingIdentifiers = "bucket_assignments",
          totalBuckets = totalBuckets
        ),
        "",
        // Compute the bucket sizes
        computeBucketSizes(
          groupingIdentifiers = "bucket_assignments",
          bucketToCount = "bucket_counts",
          totalBuckets = totalBuckets
        ),
        "",
        "// Write out the number of buckets",
        s"sets[0] = ${totalBuckets};",
        "",
        // Copy the elements to their respective buckets
        (outputs, inputs).zipped.map(
          copyVecToBucketsStmt(_, _, totalBuckets, "bucket_assignments", "bucket_counts")
        )
      )
    )
  }
}
