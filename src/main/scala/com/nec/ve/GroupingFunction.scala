package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.{CExpressionEvaluation, CFunction2}
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeScalarType, VeType}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
import com.nec.ve.GroupingFunction.DataDescription.KeyOrValue

object GroupingFunction {

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

  def computeBuckets(cVectors: List[CVector], groupingIdentifiers: String): CodeLines =
    CodeLines.from(
      s"std::vector<int> $groupingIdentifiers;",
      CodeLines.forLoop("i", s"${cVectors.head.name}->count") {
        CodeLines.from(
          s"int hash = 1;",
          cVectors.map(cVector => CodeLines.from(s"hash = 31 * ${cVector.name}->data[i];")),
          s"$groupingIdentifiers.push(hash);"
        )
      }
    )

  def computeBucketSizes(
    groupingIdentifiers: String,
    bucketToCount: String,
    totalBuckets: Int
  ): CodeLines = CodeLines.from(
    s"std::vector $bucketToCount($totalBuckets);",
    CodeLines.forLoop("i", s"${groupingIdentifiers}.size()")(
      CodeLines.from(s"int group = ${groupingIdentifiers}[i];", s"$bucketToCount[group]++;")
    )
  )

  def groupData(data: List[DataDescription], totalBuckets: Int): CFunction2 = {

    val inputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"${isKey.renderValue}_$idx")
    }

    val outputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"output_${isKey.renderValue}_$idx")
    }

    CFunction2(
      arguments = List(
        inputs.map(cVector => PointerPointer(cVector)),
        List(CFunctionArgument.Raw("int* sets")),
        outputs.map(cVector => PointerPointer(cVector))
      ).flatten,
      body = CodeLines
        .from(
          computeBuckets(
            cVectors = data.zip(inputs).filter(_._1.keyOrValue.isKey).map(_._2),
            groupingIdentifiers = "idToBucket"
          ),
          computeBucketSizes(
            groupingIdentifiers = "idToBucket",
            bucketToCount = "bucketToCount",
            totalBuckets = totalBuckets
          ),
          /** For each bucket, initialize each output vector */
          s"sets[0] = ${totalBuckets};",
          data.zip(inputs).zip(outputs).map { case ((dataDesc, input), output) =>
            CodeLines.from(
              s"*${output.name} = (${output.veType.cVectorType})malloc(sizeof(void *) * ${totalBuckets});",
              CodeLines.forLoop("b", s"${totalBuckets}") {
                CodeLines.from(
                  s"${output.name}[b] = (${output.veType.cVectorType}*)malloc(sizeof(${output.veType.cVectorType}));",
                  initializeScalarVector(
                    veScalarType = output.veType.asInstanceOf[VeScalarType],
                    variableName = s"${output.name}[b]",
                    countExpression = s"bucketToCount[b]"
                  ),
                  "int o = 0;",
                  CodeLines.forLoop("i", s"idToBucket.size()") {
                    CodeLines.ifStatement("b == idToBucket[i]")(
                      CodeLines.from(
                        s"${output.name}[b]->data[o] = ${input.name}->data[i];",
                        s"set_validity(${output.name}[b]->validityBuffer, o, 1);",
                        "o++;"
                      )
                    )
                  }
                )
              }
            )
          }
        )
    )
  }
}
