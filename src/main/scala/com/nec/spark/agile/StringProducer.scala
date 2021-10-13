package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CExpression

trait StringProducer extends Serializable {
  def produceTo(tempStringName: String, lenName: String): CodeLines
}

object StringProducer {

  def copyString(inputName: String): StringProducer = CopyStringProducer(inputName)

  final case class CopyStringProducer(inputName: String) extends StringProducer {
    override def produceTo(tsn: String, iln: String): CodeLines = CodeLines.from(
      s"std::string sub_str = std::string(${inputName}->data, ${inputName}->offsets[i], ${inputName}->offsets[i+1] - ${inputName}->offsets[i]);",
      s"${tsn}.append(sub_str);",
      s"${iln} += sub_str.size();"
    )
  }

  final case class StringChooser(condition: CExpression, ifTrue: String, otherwise: String)
    extends StringProducer {
    override def produceTo(tempStringName: String, lenName: String): CodeLines =
      CodeLines.from(
        // note: does not escape strings
        s"""std::string sub_str = ${condition.cCode} ? std::string("${ifTrue}") : std::string("${otherwise}");""",
        s"${tempStringName}.append(sub_str);",
        s"${lenName} += sub_str.size();"
      )
  }

  final case class FilteringProducer(inputName: String, outputName: String, stringProducer: StringProducer) {
    val tmpData = s"${outputName}_data";
    val tmpOffsets = s"${outputName}_tmp_offsets";
    val tmpLengths = s"${outputName}_tmp_lengths";
    val tmpCurrentOffset = s"${outputName}_tmp_current_offset";
    val tmpCount = s"${outputName}_tmp_count";
    val tmpDataInit = stringProducer match {
      case CopyStringProducer(producerInput) =>
        s"""${producerInput}"""
      case _ =>
        s"""${inputName}"""
    }

    def setup: CodeLines = {
      CodeLines.from(
        CodeLines.debugHere,
        s"""frovedis::words ${tmpData} = varchar_vector_to_words(${tmpDataInit});""",
        s"""std::vector<size_t> ${tmpData}_new_starts(${tmpData}.starts.size());""",
        s"""std::vector<size_t> ${tmpData}_new_lens(${tmpData}.lens.size());"""
      )
    }

    def forEach: CodeLines =
      CodeLines.from(
        s"""${tmpData}_new_starts[g] = ${tmpData}.starts[i];""",
        s"""${tmpData}_new_lens[g] = ${tmpData}.lens[i];"""
      )

    def complete: CodeLines = CodeLines.from(
      CodeLines.debugHere,
      s"""std::vector<size_t> ${tmpData}_unused;""",
      s"""std::vector<int> ${tmpData}_new_chars = frovedis::concat_words(${tmpData}.chars, ${tmpData}_new_starts, ${tmpData}_new_lens, "", ${tmpData}_unused);""",
      s"""${tmpData}.chars = ${tmpData}_new_chars;""",
      s"""${tmpData}.starts = ${tmpData}_new_starts;""",
      s"""${tmpData}.lens = ${tmpData}_new_lens;""",
      s"""words_to_varchar_vector(${tmpData}, ${outputName});""",
    )

    def validityForEach(idx: String): CodeLines =
      CodeLines.from(s"//set_validity(${outputName}->validityBuffer, ${idx}, 1);")
  }

  def produceVarChar(
    count: String,
    outputName: String,
    stringProducer: StringProducer
  ): CodeLines = {
    val fp = FilteringProducer("sdf", outputName, stringProducer)
    CodeLines.from(
      fp.setup,
      s"""for ( int32_t i = 0; i < ${count}; i++ ) {""",
      fp.forEach.indented,
      "}",
      fp.complete,
      s"for( int32_t i = 0; i < ${count}; i++ ) {",
      CodeLines.from(fp.validityForEach("i")).indented,
      "}"
    )
  }
}
