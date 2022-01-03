package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.groupby.GroupByOutline.initializeScalarVector
import com.nec.spark.agile.join.GenericJoiner._

final case class GenericJoiner(
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  joins: List[Join],
  outputs: List[FilteredOutput]
) {
  val jbe = JoinByEquality(inputsLeft, inputsRight, joins)
  val io: List[CVector] = inputsLeft ++ inputsRight ++ outputs.map(_.cVector)
  def produce(fName: String): CodeLines = CodeLines.from(
    jbe.produceIndices("compute_indices"),
    s"""extern "C" long ${fName}(""",
    io
      .map(_.declarePointer)
      .mkString(",\n"),
    ") {",
    "nullable_int_vector left_idx;",
    "nullable_int_vector right_idx;",
    CodeLines
      .from(
        s"compute_indices(${{
          jbe.ioWo.map(_.name) ++
            jbe.ioO.map(v => s"&${v.name}")
        }.mkString(", ")});",
        s"std::vector<size_t> left_idx_std = idx_to_std(&left_idx);",
        s"std::vector<size_t> right_idx_std = idx_to_std(&right_idx);",
        outputs.reverse
          .map {
            case FilteredOutput(newName, source @ CScalarVector(name, veType)) =>
              val isLeft = inputsLeft.contains(source)
              val indicesName = if (isLeft) "left_idx_std" else "right_idx_std"
              populateScalar(
                outputName = newName,
                inputIndices = indicesName,
                inputName = name,
                veScalarType = veType
              )
            case FilteredOutput(newName, source @ CVarChar(name)) =>
              val isLeft = inputsLeft.contains(source)
              val indicesName = if (isLeft) "left_idx_std" else "right_idx_std"

              CodeLines.from(
                s"""std::cout << " A " << std::endl << std::flush;""",
                s"auto ${name}_words = varchar_vector_to_words(${name});",
                s"""std::cout << " D " << std::endl << std::flush;""",
                s"auto ${name}_new_words = filter_words(${name}_words, ${indicesName});",
                s"words_to_varchar_vector(${name}_new_words, ${name});",
                s"""std::cout << " E " << std::endl << std::flush;"""
              )
          }
          .zipWithIndex
          .map { case (cl, i) =>
            CodeLines.from(
              s"""std::cout << $i << " -- " << std::endl << std::flush;""",
              cl,
              s"""std::cout << $i << " /-- " << std::endl << std::flush;"""
            )
          },
        "return 0;"
      )
      .indented,
    "}"
  )
}

object GenericJoiner {

  final case class FilteredOutput(outputName: String, source: CVector) {
    def cVector: CVector = source.withNewName(outputName)
  }

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

  val MatchingIndicesVec = "matching_indices"

  def populateVarChar(dict: String, inputIndices: String, outputName: String): CodeLines =
    CodeLines.from(
      s"""words_to_varchar_vector(${dict}.index_to_words(${inputIndices}), ${outputName});"""
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

  final case class EqualityPairing(indexOfFirstColumn: String, indexOfSecondColumn: String) {
    def toCondition: String = s"$indexOfFirstColumn[i] == $indexOfSecondColumn[j]"
  }

  /**
   * This combines joins from multiple places to produce corresponding indices for output items
   */
  private def computeMatchingIndices(
    outMatchingIndices: String,
    firstPairing: EqualityPairing,
    secondPairing: EqualityPairing
  ): CodeLines =
    CodeLines
      .from(
        s"std::vector<size_t> ${outMatchingIndices};",
        s"for (int i = 0; i < ${firstPairing.indexOfFirstColumn}.size(); i++) {",
        s"  for (int j = 0; j < ${secondPairing.indexOfSecondColumn}.size(); j++) {",
        s"    if (${firstPairing.toCondition} && ${secondPairing.toCondition}) {",
        s"      ${outMatchingIndices}.push_back(i);",
        "    }",
        "  }",
        "}"
      )

  def computeNumJoin(
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

  def computeStringJoin(
    inLeftWords: String,
    inLeftDict: String,
    leftDictIndices: String,
    outMatchingIndicesLeft: String,
    outMatchingIndicesRight: String,
    inRightVarChar: String
  ): CodeLines =
    CodeLines.from(
      s"std::vector<size_t> ${outMatchingIndicesLeft};",
      s"std::vector<size_t> ${outMatchingIndicesRight};",
      s"std::vector<size_t> ${leftDictIndices} = $inLeftDict.lookup(frovedis::make_compressed_words(${inLeftWords}));",
      CodeLines
        .from(
          s"std::vector<size_t> left_idx($leftDictIndices.size());",
          s"for (int i = 0; i < $leftDictIndices.size(); i++) {",
          s"  left_idx[i] = i;",
          s"}",
          s"std::vector<size_t> right = $inLeftDict.lookup(frovedis::make_compressed_words(varchar_vector_to_words(${inRightVarChar})));",
          s"std::vector<size_t> right_idx(right.size());",
          s"for (int i = 0; i < right.size(); i++) {",
          s"  right_idx[i] = i;",
          s"}",
          s"frovedis::equi_join(right, right_idx, $leftDictIndices, left_idx, ${outMatchingIndicesRight}, ${outMatchingIndicesLeft});"
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
