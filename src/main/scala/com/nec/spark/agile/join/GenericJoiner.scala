package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
import com.nec.spark.agile.join.GenericJoiner.Output.{ScalarOutput, StringOutput}
import com.nec.spark.agile.join.GenericJoiner.OutputSpec.{ScalarOutputSpec, StringOutputSpec}
import com.nec.spark.agile.join.GenericJoiner._

final case class GenericJoiner(
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  firstJoin: Join,
  secondJoin: Join
) {
  def produce: CodeLines =
    CodeLines.from(
      """#include "frovedis/core/radix_sort.hpp"""",
      """#include "frovedis/dataframe/join.hpp"""",
      """#include "frovedis/dataframe/join.cc"""",
      """#include "frovedis/text/words.hpp"""",
      """#include "frovedis/text/words.cc"""",
      """#include "frovedis/text/dict.hpp"""",
      """#include "frovedis/text/dict.cc"""",
      """#include <iostream>""",
      """#include <vector>""",
      """#include <cmath>""",
      printVec,
      """extern "C" long adv_join(""",
      (inputs ++ outputs
        .map(_.cVector))
        .map(_.declarePointer)
        .mkString(",\n"),
      ")",
      """{""",
      CodeLines
        .from(
          dictsWords.commented("Create dicts"),
          firstJoinCode.commented("First join code"),
          secondJoinCode.commented("Second join code"),
          computeMatchingIndices(
            outMatchingIndices = MatchingIndicesVec,
            firstLeft = firstPairing.indexOfFirstColumn,
            secondLeft = firstPairing.indexOfSecondColumn,
            firstPairing = firstPairing,
            secondPairing = secondPairing
          ).commented("Compute left & right indices"),
          CodeLines
            .from(
              outputs.map(out =>
                out
                  .produce(MatchingIndicesVec)
                  .indented
                  .commented(s"Produce ${out.cVector.name}")
              )
            )
            .commented("Produce outputs"),
          "return 0;"
        )
        .indented,
      """}"""
    )

  def varCharMatchingIndicesLeftDict = s"dict_indices_${inputsLeft(0).name}"

  private def firstPairing =
    EqualityPairing(
      indexOfFirstColumn = s"index_${firstJoin.left.name}",
      indexOfSecondColumn = s"index_${firstJoin.right.name}"
    )

  private def secondPairing =
    EqualityPairing(
      indexOfFirstColumn = s"index_${secondJoin.left.name}",
      indexOfSecondColumn = s"index_${secondJoin.right.name}"
    )

  private def left_words = s"words_${firstJoin.left.name}"

  private def left_dict = s"dict_${firstJoin.left.name}"

  private def firstJoinCode = computeStringJoin(
    outLeftDictIndices = varCharMatchingIndicesLeftDict,
    outMatchingIndicesLeft = firstPairing.indexOfFirstColumn,
    outMatchingIndicesRight = secondPairing.indexOfFirstColumn,
    inLeftDict = left_dict,
    inLeftWords = left_words,
    inRightVarChar = firstJoin.right.name
  )

  private def secondJoinCode = computeNumJoin(
    outMatchingIndicesLeft = firstPairing.indexOfSecondColumn,
    inLeft = secondJoin.left.name,
    outMatchingIndicesRight = secondPairing.indexOfSecondColumn,
    inRight = secondJoin.right.name
  )

  private def inputs = inputsLeft ++ inputsRight

  /** Specific to this case - to make generic we may have to de-optimize */
  private def dictsWords = CodeLines
    .from(
      s"frovedis::words ${left_words} = varchar_vector_to_words(${firstJoin.left.name});",
      s"frovedis::dict ${left_dict} = frovedis::make_dict(${left_words});"
    )

  private def firstColSpec =
    StringOutputSpec(outputName = "o_a", inputName = inputsLeft(0).name)

  private def secondColSpec =
    ScalarOutputSpec(
      outputName = "o_b",
      inputName = inputsLeft(2).name,
      veScalarType = inputsLeft(2).veType.asInstanceOf[VeScalarType]
    )

  private def thirdColSpec = ScalarOutputSpec(
    outputName = "o_c",
    inputName = inputsRight(2).name,
    veScalarType = inputsRight(2).veType.asInstanceOf[VeScalarType]
  )

  def outputs: List[Output] = List(
    StringOutput(
      outputName = firstColSpec.outputName,
      sourceIndex = s"${varCharMatchingIndicesLeftDict}[${firstPairing.indexOfFirstColumn}[i]]",
      sourceDict = left_dict,
      inMatchingDictIndices = varCharMatchingIndicesLeftDict
    ),
    ScalarOutput(
      source = secondColSpec.inputName,
      sourceIndex = s"${firstPairing.indexOfFirstColumn}[i]",
      outputName = secondColSpec.outputName,
      veType = secondColSpec.veScalarType
    ),
    ScalarOutput(
      source = thirdColSpec.inputName,
      sourceIndex = s"${secondPairing.indexOfFirstColumn}[i]",
      outputName = thirdColSpec.outputName,
      veType = thirdColSpec.veScalarType
    )
  )

}

object GenericJoiner {

  sealed trait OutputSpec {
    def outputName: String
    def inputName: String
    def veType: VeType
    def inputVector: CVector
    def outputVector: CVector
  }
  object OutputSpec {
    final case class StringOutputSpec(outputName: String, inputName: String) extends OutputSpec {
      override def veType: VeType = VeString
      override def inputVector: CVector = CVector.varChar(inputName)
      override def outputVector: CVector = CVector.varChar(outputName)
    }
    final case class ScalarOutputSpec(
      outputName: String,
      inputName: String,
      veScalarType: VeScalarType
    ) extends OutputSpec {
      override def veType: VeType = veScalarType
      override def inputVector: CVector = veScalarType.makeCVector(inputName)
      override def outputVector: CVector = veScalarType.makeCVector(outputName)
    }
  }

  val MatchingIndicesVec = "left_indices"

  def populateVarChar(leftDict: String, inputIndices: String, outputName: String): CodeLines =
    CodeLines.from(
      s"""words_to_varchar_vector(${leftDict}.index_to_words(${inputIndices}), ${outputName});"""
    )

  def populateScalar(
    outputName: String,
    inputIndices: String,
    inputName: String,
    veScalarType: VeScalarType
  ): CodeLines =
    CodeLines.from(
      initializeScalarVector(veScalarType, outputName, s"${inputIndices}.size()"),
      CodeLines.forLoop("i", s"${inputIndices}.size()")(
        CodeLines.from(
          s"${outputName}->data[i] = ${inputName}->data[${inputIndices}[i]];",
          s"set_validity(${outputName}->validityBuffer, i, check_valid(${inputName}->validityBuffer, ${inputIndices}[i]));"
        )
      )
    )

  final case class Join(left: CVector, right: CVector)

  sealed trait Output {
    def cVector: CVector
    def produce(matchingIndicesVec: String): CodeLines
  }
  object Output {

    final case class ScalarOutput(
      source: String,
      outputName: String,
      sourceIndex: String,
      veType: VeScalarType
    ) extends Output {
      def produce(matchingIndicesVec: String): CodeLines = CodeLines
        .from(
          s"std::vector<size_t> input_indices;",
          s"for (int x = 0; x < $matchingIndicesVec.size(); x++) {",
          s"  int i = $matchingIndicesVec[x];",
          s"  input_indices.push_back(${sourceIndex});",
          "}",
          populateScalar(
            outputName = outputName,
            inputIndices = "input_indices",
            inputName = source,
            veScalarType = veType
          )
        )
        .block

      def cVector: CVector = CVector(outputName, veType)
    }

    final case class StringOutput(
      outputName: String,
      sourceIndex: String,
      inMatchingDictIndices: String,
      sourceDict: String
    ) extends Output {

      def produce(matchingIndicesVec: String): CodeLines = CodeLines
        .from(
          s"std::vector<size_t> input_indices;",
          s"for (int x = 0; x < $matchingIndicesVec.size(); x++) {",
          s"  int i = $matchingIndicesVec[x];",
          s"  input_indices.push_back(${sourceIndex});",
          "}",
          populateVarChar(
            leftDict = sourceDict,
            inputIndices = "input_indices",
            outputName = outputName
          )
        )
        .block

      def cVector: CVector = CVarChar(outputName)
    }

  }

  final case class EqualityPairing(indexOfFirstColumn: String, indexOfSecondColumn: String) {
    def toCondition: String = s"$indexOfFirstColumn[i] == $indexOfSecondColumn[j]"
  }

  /**
   * This combines joins from multiple places to produce corresponding indices for output items
   */
  private def computeMatchingIndices(
    outMatchingIndices: String,
    firstLeft: String,
    secondLeft: String,
    firstPairing: EqualityPairing,
    secondPairing: EqualityPairing
  ): CodeLines =
    CodeLines
      .from(
        s"std::vector<size_t> ${outMatchingIndices};",
        s"for (int i = 0; i < $firstLeft.size(); i++) {",
        s"  for (int j = 0; j < $secondLeft.size(); j++) {",
        s"    if (${firstPairing.toCondition} && ${secondPairing.toCondition}) {",
        s"      ${outMatchingIndices}.push_back(i);",
        "    }",
        "  }",
        "}"
      )

  private def computeNumJoin(
    outMatchingIndicesLeft: String,
    outMatchingIndicesRight: String,
    inLeft: String,
    inRight: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${outMatchingIndicesLeft};",
      s"std::vector<size_t> ${outMatchingIndicesRight};",
      CodeLines
        .from(
          s"std::vector<int64_t> left(${inLeft}->count);",
          s"std::vector<size_t> left_idx(${inLeft}->count);",
          s"for (int i = 0; i < ${inLeft}->count; i++) {",
          s"  left[i] = ${inLeft}->data[i];",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<int64_t> right(${inRight}->count);",
          s"std::vector<size_t> right_idx(${inRight}->count);",
          s"for (int i = 0; i < ${inRight}->count; i++) {",
          s"  right[i] = ${inRight}->data[i];",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, left, left_idx, $outMatchingIndicesRight, $outMatchingIndicesLeft);"
        )
        .block
    )

  private def computeStringJoin(
    inLeftWords: String,
    inLeftDict: String,
    outLeftDictIndices: String,
    outMatchingIndicesLeft: String,
    outMatchingIndicesRight: String,
    inRightVarChar: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${outMatchingIndicesLeft};",
      s"std::vector<size_t> ${outMatchingIndicesRight};",
      s"std::vector<size_t> ${outLeftDictIndices} = $inLeftDict.lookup(frovedis::make_compressed_words(${inLeftWords}));",
      CodeLines
        .from(
          s"std::vector<size_t> left_idx($outLeftDictIndices.size());",
          s"for (int i = 0; i < $outLeftDictIndices.size(); i++) {",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<size_t> right = $inLeftDict.lookup(frovedis::make_compressed_words(varchar_vector_to_words(${inRightVarChar})));",
          s"std::vector<size_t> right_idx(right.size());",
          s"for (int i = 0; i < right.size(); i++) {",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, $outLeftDictIndices, left_idx, ${outMatchingIndicesRight}, ${outMatchingIndicesLeft});"
        )
        .block
    )

  def printVec: CodeLines = CodeLines.from(
    """#ifdef DEBUG""",
    """template<typename T>""",
    """void print_vec(char *name, std::vector<T> a) {""",
    CodeLines
      .from(
        """std::cout << name << " = [";""",
        """char *comma = "";""",
        """for (int i = 0; i < a.size(); i++) {""",
        """std::cout << comma << a[i];""",
        """comma = ",";""",
        """}""",
        """std::cout << "]" << std::endl;"""
      )
      .indented,
    """}""",
    """#endif"""
  )
}
