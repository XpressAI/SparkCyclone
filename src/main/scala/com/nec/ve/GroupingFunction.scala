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
      s"std::vector<int> $groupingIdentifiers;",
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
      s"std::vector<int> $bucketToCount;",
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

    val copyStmt = output.veType match {
      case CFunctionGeneration.VeString =>
        val outName = s"${output.name}_current"
        val fcsp = StringProducer.FrovedisCopyStringProducer(s"${input.name}[0]")

        CodeLines.from(
          // Point to the current nullable_varchar_vector
          s"${output.veType.cVectorType} *${outName} = ${output.name}[b];",
          // Currently a no-op
          GroupByOutline.initializeStringVector(outName),
          "",
          // Set starts and lens vectors to zero but with capacity equal to the count in the bucket
          fcsp.init(outName, "0", s"${bucketToCount}[b]"),
          "",
          // Construct the starts and lens vectors
          CodeLines.forLoop("i", s"${idToBucket}.size()") {
            CodeLines.ifStatement(s"b == ${idToBucket}[i]") {
              List(
                s"${fcsp.frovedisStarts(outName)}.emplace_back(${fcsp.wordName(outName)}.starts[i]);",
                s"${fcsp.frovedisLens(outName)}.emplace_back(${fcsp.wordName(outName)}.lens[i]);"
              )
            }
          },
          "",
          // Construct the output nullable_varchar_vector using the word components
          fcsp.complete(outName),
          "",
          "auto o = 0;",
          CodeLines.forLoop("i", s"${idToBucket}.size()") {
            CodeLines.ifStatement(s"b == ${idToBucket}[i]") {
              // Preserve the validity bit values across bucketing
              s"set_validity(${outName}->validityBuffer, o++, check_valid(${input.name}[0]->validityBuffer, i));",
            }
          }
        )

      case other: VeScalarType =>
        CodeLines.from(
          // Initialize the values in the nullable_T_vector
          GroupByOutline.initializeScalarVector(
            veScalarType = other,
            variableName = s"${output.name}[b]",
            countExpression = s"${bucketToCount}[b]"
          ),
          "",
          "auto o = 0;",
          CodeLines.forLoop("i", s"${idToBucket}.size()") {
            // If element is in the current bucket
            CodeLines.ifStatement(s"b == ${idToBucket}[i]") {
              List(
                // Copy the value over
                s"${output.name}[b]->data[o] = ${input.name}[0]->data[i];",
                // Preserve the validity bit values across bucketing
                s"set_validity(${output.name}[b]->validityBuffer, o++, check_valid(${input.name}[0]->validityBuffer, i));"
              )
            }
          }
        )
    }

    CodeLines.scoped(
      s"Copy elements of ${input.name}[0] to their respective buckets in ${output.name}"
    ) {
      CodeLines.from(
        // Allocate the nullable_T_vector[] with size buckets
        s"*${output.name} = static_cast<${output.veType.cVectorType}*>(malloc(sizeof(nullptr) * ${buckets}));",
        "",
        // Loop over each bucket
        CodeLines.forLoop("b", s"${buckets}") {
          CodeLines.from(
            // Allocate the nullable_T_vector at [0]
            s"${output.name}[b] = static_cast<${output.veType.cVectorType}*>(malloc(sizeof(${output.veType.cVectorType})));",
            // Perform the copy based on type T (scalar or varchar)
            copyStmt
          )
        }
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
          groupingIdentifiers = "idToBucket",
          totalBuckets = totalBuckets
        ),
        "",
        // Compute the bucket sizes
        computeBucketSizes(
          groupingIdentifiers = "idToBucket",
          bucketToCount = "bucketToCount",
          totalBuckets = totalBuckets
        ),
        "",
        "// Write out the number of buckets",
        s"sets[0] = ${totalBuckets};",
        "",
        // Copy the elements to their respective buckets
        (outputs, inputs).zipped.map(
          copyVecToBucketsStmt(_, _, totalBuckets, "idToBucket", "bucketToCount")
        )
      )
    )
  }
}
