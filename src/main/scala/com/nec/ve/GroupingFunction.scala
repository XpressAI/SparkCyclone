package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.CFunctionGeneration.{CVector, VeScalarType, VeType}
import com.nec.spark.agile.{CFunction2, CFunctionGeneration, StringProducer}
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
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
      s"std::vector<int> $groupingIdentifiers;",
      CodeLines.forLoop("i", s"${cVectors.head.name}[0]->count") {
        CodeLines.from(
          s"int hash = 1;",
          cVectors.map(cVector =>
            CodeLines.from(
              if (cVector.veType.isString) addStringHashing(s"${cVector.name}[0]", "i", "hash")
              else CodeLines.from(s"hash = 31 * ${cVector.name}[0]->data[i];")
            )
          ),
          s"$groupingIdentifiers.push_back(hash % ${totalBuckets});"
        )
      }
    )
  }

  def computeBucketSizes(
    groupingIdentifiers: String,
    bucketToCount: String,
    totalBuckets: Int
  ): CodeLines = CodeLines.from(
    s"std::vector<int> $bucketToCount;",
    CodeLines.forLoop("g", s"$totalBuckets")(
      CodeLines.from(
        s"int cnt = 0;",
        CodeLines.forLoop("i", s"${groupingIdentifiers}.size()")(
          CodeLines.ifStatement(s"${groupingIdentifiers}[i] == g")(CodeLines.from("cnt++;"))
        ),
        s"$bucketToCount.push_back(cnt);"
      )
    )
  )

  def groupDataNoOp(data: List[DataDescription]): CFunction2 = {
    val inputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"${isKey.renderValue}_${idx}")
    }

    val outputs = data.zipWithIndex.map { case (DataDescription(veType, isKey), idx) =>
      veType.makeCVector(s"output_${isKey.renderValue}_${idx}")
    }

    val arguments = inputs.map(PointerPointer(_)) ++
      List(CFunctionArgument.Raw("int* sets")) ++
      outputs.map(PointerPointer(_))

    val copyPointerLines = (inputs, outputs).zipped.map { case (in, out) =>
      // Make the output pointers point to the input data structures
      s"*${out.name} = ${in.name}[0];"
    }.toList

    CFunction2(arguments, CodeLines.from(s"sets[0] = 1;", copyPointerLines))
  }

  def groupData(data: List[DataDescription], totalBuckets: Int) = {
    if (data.count(_.keyOrValue.isKey) <= 0) {
      groupDataNoOp(data)
    } else {
      groupDataImpl(data, totalBuckets)
    }
  }

  def groupDataImpl(data: List[DataDescription], totalBuckets: Int): CFunction2 = {
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
      body = CodeLines
        .from(
          computeBuckets(
            cVectors = data.zip(inputs).filter(_._1.keyOrValue.isKey).map(_._2),
            groupingIdentifiers = "idToBucket",
            totalBuckets = totalBuckets
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
              s"*${output.name} = (${output.veType.cVectorType}*)malloc(sizeof(void *) * ${totalBuckets});",
              CodeLines.forLoop("b", s"${totalBuckets}") {
                CodeLines.from(
                  s"${output.name}[b] = (${output.veType.cVectorType}*)malloc(sizeof(${output.veType.cVectorType}));",
                  output.veType match {
                    case CFunctionGeneration.VeString =>
                      val outName = s"${output.name}_current"
                      val fp =
                        StringProducer.FilteringProducer(
                          outName,
                          StringProducer.ImpCopyStringProducer(s"${input.name}[0]")
                        )
                      CodeLines
                        .from(
                          s"${output.veType.cVectorType} * $outName = ${output.name}[b];",
                          CodeLines.debugHere,
                          GroupByOutline.initializeStringVector(outName),
                          CodeLines.debugHere,
                          fp.setup,
                          CodeLines.debugHere,
                          "int o = 0;",
                          CodeLines.forLoop("i", s"idToBucket.size()") {
                            CodeLines.from(
                              CodeLines.ifStatement("b == idToBucket[i]")(
                                CodeLines.from(fp.forEach, "o++;")
                              )
                            )
                          },
                          fp.complete,
                          "o = 0;",
                          CodeLines.forLoop("i", s"idToBucket.size()") {
                            CodeLines.from(
                              CodeLines.ifStatement("b == idToBucket[i]")(
                                CodeLines.from(fp.validityForEach("o"), "o++;")
                              )
                            )
                          }
                        )
                        .blockCommented("String")

                    case other: VeScalarType =>
                      CodeLines.from(
                        initializeScalarVector(
                          veScalarType = other,
                          variableName = s"${output.name}[b]",
                          countExpression = s"bucketToCount[b]"
                        ),
                        "int o = 0;",
                        CodeLines.forLoop("i", s"idToBucket.size()") {
                          CodeLines.ifStatement("b == idToBucket[i]") {
                            List(
                              s"${output.name}[b]->data[o] = ${input.name}[0]->data[i];",
                              s"set_validity(${output.name}[b]->validityBuffer, o, 1);",
                              "o++;"
                            )
                          }
                        }
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
