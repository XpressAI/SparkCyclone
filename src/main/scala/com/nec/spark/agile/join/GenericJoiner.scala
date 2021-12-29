package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CVarChar, CVector, VeScalarType, VeString, VeType}
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
import com.nec.spark.agile.join.GenericJoiner.Output.{ScalarOutput, StringOutput}

object GenericJoiner {

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

  def produce(
    inputsLeft: List[CVector],
    inputsRight: List[CVector],
    firstJoin: Join,
    secondJoin: Join
  ): CodeLines = {

    val varCharMatchingIndicesLeftDict = s"dict_indices_${inputsLeft(0).name}"

    val firstPairing =
      EqualityPairing(
        indexOfFirstColumn = s"index_${firstJoin.left.name}",
        indexOfSecondColumn = s"index_${firstJoin.right.name}"
      )

    val secondPairing =
      EqualityPairing(
        indexOfFirstColumn = s"index_${secondJoin.left.name}",
        indexOfSecondColumn = s"index_${secondJoin.right.name}"
      )

    val left_words = s"words_${firstJoin.left.name}"
    val left_dict = s"dict_${firstJoin.left.name}"

    val firstJoinCode = computeStringJoin(
      outLeftDictIndices = varCharMatchingIndicesLeftDict,
      outMatchingIndicesLeft = firstPairing.indexOfFirstColumn,
      outMatchingIndicesRight = secondPairing.indexOfFirstColumn,
      inLeftDict = left_dict,
      inLeftWords = left_words,
      inRightVarChar = firstJoin.right.name
    )

    val secondJoinCode = computeNumJoin(
      outMatchingIndicesLeft = firstPairing.indexOfSecondColumn,
      inLeft = secondJoin.left.name,
      outMatchingIndicesRight = secondPairing.indexOfSecondColumn,
      inRight = secondJoin.right.name
    )

    val inputs = inputsLeft ++ inputsRight

    /** Specific to this case - to make generic we may have to de-optimize */
    val dictsWords = CodeLines
      .from(
        s"frovedis::words ${left_words} = varchar_vector_to_words(${firstJoin.left.name});",
        s"frovedis::dict ${left_dict} = frovedis::make_dict(${left_words});"
      )

    val outputs: List[Output] = List(
      StringOutput(
        outputName = "o_a_var",
        sourceIndex = s"${varCharMatchingIndicesLeftDict}[${firstPairing.indexOfFirstColumn}[i]]",
        inMatchingDictIndices = varCharMatchingIndicesLeftDict,
        sourceDict = left_dict
      ),
      ScalarOutput(
        source = inputsLeft(2).name,
        outputName = "o_b_var",
        sourceIndices = firstPairing.indexOfFirstColumn,
        veType = VeScalarType.VeNullableInt
      ),
      ScalarOutput(
        source = inputsRight(2).name,
        outputName = "o_c_var",
        sourceIndices = secondPairing.indexOfFirstColumn,
        veType = VeScalarType.VeNullableDouble
      )
    )

    val leftIndicesVec = "lel"
    val rightIndicesVec = "rel"

    val outIndexComputations =
      computeLeftRightIndices(
        outMatchingLeftIndices = leftIndicesVec,
        outMatchingRightIndices = rightIndicesVec,
        firstLeft = firstPairing.indexOfFirstColumn,
        secondLeft = firstPairing.indexOfSecondColumn,
        firstPairing = firstPairing,
        secondPairing = secondPairing
      )

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
          dictsWords,
          firstJoinCode,
          secondJoinCode,
          outIndexComputations,
          outputs.map(_.produce(leftIndicesVec, rightIndicesVec)),
          "return 0;"
        )
        .indented,
      """}"""
    )
  }

  sealed trait Output {
    def cVector: CVector
    def produce(leftIndicesVec: String, rightIndicesVec: String): CodeLines
  }
  object Output {

    final case class ScalarOutput(
      source: String,
      outputName: String,
      sourceIndices: String,
      veType: VeScalarType
    ) extends Output {
      def sourceIndex = s"${sourceIndices}[i]"

      def produce(leftIndicesVec: String, rightIndicesVec: String): CodeLines = CodeLines
        .from(
          s"std::vector<size_t> input_indices;",
          s"for (int x = 0; x < $leftIndicesVec.size(); x++) {",
          s"  int i = $leftIndicesVec[x];",
          s"  int j = $rightIndicesVec[x];",
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

      def produce(leftIndicesVec: String, rightIndicesVec: String): CodeLines = CodeLines
        .from(
          s"std::vector<size_t> input_indices;",
          s"for (int x = 0; x < $leftIndicesVec.size(); x++) {",
          s"  int i = $leftIndicesVec[x];",
          s"  int j = $rightIndicesVec[x];",
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
  private def computeLeftRightIndices(
    outMatchingLeftIndices: String,
    outMatchingRightIndices: String,
    firstLeft: String,
    secondLeft: String,
    firstPairing: EqualityPairing,
    secondPairing: EqualityPairing
  ): CodeLines =
    CodeLines
      .from(
        s"std::vector<size_t> ${outMatchingLeftIndices};",
        s"std::vector<size_t> ${outMatchingRightIndices};",
        s"for (int i = 0; i < $firstLeft.size(); i++) {",
        s"  for (int j = 0; j < $secondLeft.size(); j++) {",
        s"    if (${firstPairing.toCondition} && ${secondPairing.toCondition}) {",
        s"      ${outMatchingLeftIndices}.push_back(i);",
        s"      ${outMatchingRightIndices}.push_back(j);",
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
