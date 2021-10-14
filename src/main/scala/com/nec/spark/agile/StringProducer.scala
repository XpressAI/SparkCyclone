package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CExpression

sealed trait StringProducer extends Serializable {}

object StringProducer {

  trait ImperativeStringProducer extends StringProducer {
    def produceTo(tempStringName: String, lenName: String): CodeLines
  }

  trait FrovedisStringProducer extends StringProducer {
    def produceTo(stringVectorName: String): CodeLines
  }

  def copyString(inputName: String): StringProducer = CopyStringProducer(inputName)

  final case class CopyStringProducer(inputName: String) extends FrovedisStringProducer {
    def produceTo(stringVectorName: String): CodeLines = CodeLines.from(
      s"${stringVectorName}.push_back(std::string(${inputName}->data, ${inputName}->offsets[i], ${inputName}->offsets[i+1] - ${inputName}->offsets[i]));"
    )
  }

  final case class StringChooser(condition: CExpression, ifTrue: String, otherwise: String)
    extends ImperativeStringProducer {
    override def produceTo(tempStringName: String, lenName: String): CodeLines =
      CodeLines.from(
        // note: does not escape strings
        s"""std::string sub_str = ${condition.cCode} ? std::string("${ifTrue}") : std::string("${otherwise}");""",
        s"${tempStringName}.append(sub_str);",
        s"${lenName} += sub_str.size();"
      )
  }

  final case class FilteringProducer(outputName: String, stringProducer: StringProducer) {
    val tmpString = s"${outputName}_tmp";
    val tmpOffsets = s"${outputName}_tmp_offsets";
    val tmpCurrentOffset = s"${outputName}_tmp_current_offset";
    val tmpCount = s"${outputName}_tmp_count";

    val frovedisTmpVector = s"${outputName}_tmp_vector"
    def setup: CodeLines =
      stringProducer match {
        case _: ImperativeStringProducer =>
          CodeLines.from(
            CodeLines.debugHere,
            s"""std::string ${tmpString}("");""",
            s"""std::vector<int32_t> ${tmpOffsets};""",
            s"""int32_t ${tmpCurrentOffset} = 0;""",
            s"int ${tmpCount} = 0;"
          )
        case _: FrovedisStringProducer =>
          CodeLines.from(
            CodeLines.debugHere,
            s"""std::vector<std::string> ${frovedisTmpVector}(0);"""
          )
      }

    def forEach: CodeLines = {
      stringProducer match {
        case imperative: ImperativeStringProducer =>
          CodeLines
            .from(
              CodeLines.debugHere,
              "int len = 0;",
              imperative.produceTo(s"${tmpString}", "len"),
              s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
              s"""${tmpCurrentOffset} += len;""",
              s"${tmpCount}++;"
            )
        case frovedisStringProducer: FrovedisStringProducer =>
          CodeLines
            .from(CodeLines.debugHere, frovedisStringProducer.produceTo(frovedisTmpVector))
      }
    }

    def complete: CodeLines =
      stringProducer match {
        case _: ImperativeStringProducer =>
          CodeLines.from(
            CodeLines.debugHere,
            s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
            s"""${outputName}->count = ${tmpCount};""",
            s"""${outputName}->size = ${tmpCurrentOffset};""",
            s"""${outputName}->data = (char*)malloc(${outputName}->size);""",
            s"""memcpy(${outputName}->data, ${tmpString}.data(), ${outputName}->size);""",
            s"""${outputName}->offsets = (int32_t*)malloc(sizeof(int32_t) * (${outputName}->count + 1));""",
            s"""memcpy(${outputName}->offsets, ${tmpOffsets}.data(), sizeof(int32_t) * (${outputName}->count + 1));""",
            s"${outputName}->validityBuffer = (uint64_t *) malloc(ceil(${outputName}->count / 64.0) * sizeof(uint64_t));",
            CodeLines.debugHere
          )

        case _: FrovedisStringProducer =>
          CodeLines.from(
            CodeLines.debugHere,
            s"words_to_varchar_vector(frovedis::vector_string_to_words(${frovedisTmpVector}), ${outputName});",
          )
      }

    def validityForEach(idx: String): CodeLines =
      CodeLines.from(s"set_validity(${outputName}->validityBuffer, ${idx}, 1);")
  }

  def produceVarChar(
    count: String,
    outputName: String,
    stringProducer: StringProducer
  ): CodeLines = {
    val fp = FilteringProducer(outputName, stringProducer)
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
