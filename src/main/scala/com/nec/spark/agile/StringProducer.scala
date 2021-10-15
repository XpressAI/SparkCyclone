package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.CExpression

sealed trait StringProducer extends Serializable {}

object StringProducer {

  trait ImperativeStringProducer extends StringProducer {
    def produceTo(tempStringName: String, lenName: String): CodeLines
  }

  trait FrovedisStringProducer extends StringProducer {
    def init(outputName: String, size: String): CodeLines
    def produce(outputName: String): CodeLines
    def complete(outputName: String): CodeLines
  }

  def copyString(inputName: String): StringProducer = FrovedisCopyStringProducer(inputName)

//  def copyString(inputName: String): StringProducer = ImpCopyStringProducer(inputName)

  private final case class ImpCopyStringProducer(inputName: String)
    extends ImperativeStringProducer
    with CopyStringProducer {

    override def produceTo(tsn: String, iln: String): CodeLines = {
      CodeLines.from(
        s"std::string sub_str = std::string(${inputName}->data, ${inputName}->offsets[i], ${inputName}->offsets[i+1] - ${inputName}->offsets[i]);",
        s"${tsn}.append(sub_str);",
        s"${iln} += sub_str.size();"
      )
    }
  }

  sealed trait CopyStringProducer {
    def inputName: String
  }

  private final case class FrovedisCopyStringProducer(inputName: String)
    extends FrovedisStringProducer
    with CopyStringProducer {
    def frovedisStarts(outputName: String) = s"${outputName}_starts"

    def frovedisLens(outputName: String) = s"${outputName}_lens"

    def wordName(outputName: String) = s"${outputName}_words"
    def newChars(outputName: String) = s"${outputName}_new_chars"
    def newStarts(outputName: String) = s"${outputName}_new_starts"

    def produce(outputName: String): CodeLines =
      CodeLines.from(
        s"${frovedisStarts(outputName)}[g] = ${wordName(outputName)}.starts[i];",
        s"${frovedisLens(outputName)}[g] = ${wordName(outputName)}.lens[i];"
      )

    override def init(outputName: String, size: String): CodeLines =
      CodeLines.from(
        s"frovedis::words ${wordName(outputName)} = varchar_vector_to_words(${inputName});",
        s"""std::vector<size_t> ${frovedisStarts(outputName)}(${size});""",
        s"""std::vector<size_t> ${frovedisLens(outputName)}(${size});"""
      )

    override def complete(outputName: String): CodeLines = CodeLines.from(
      s"""std::vector<size_t> ${newStarts(outputName)};""",
      s"""std::vector<int> ${newChars(outputName)} = concat_words(${wordName(
        outputName
      )}, "", ${newStarts(outputName)});""",
      s"""${wordName(outputName)}.chars = ${newChars(outputName)};""",
      s"""${wordName(outputName)}.starts = ${newStarts(outputName)};""",
      s"""${wordName(outputName)}.lens = ${frovedisLens(outputName)};""",
      s"words_to_varchar_vector(${wordName(outputName)}, ${outputName});"
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
        case f: FrovedisStringProducer =>
          CodeLines.from(CodeLines.debugHere, f.init(outputName, "groups_count"))
      }

    def forEach: CodeLines = {
      stringProducer match {
        case imperative: ImperativeStringProducer =>
          CodeLines
            .from(
              CodeLines.debugHere,
              "int len = 0;",
              imperative.produceTo(s"$tmpString", "len"),
              s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
              s"""${tmpCurrentOffset} += len;""",
              s"${tmpCount}++;"
            )
        case frovedisStringProducer: FrovedisStringProducer =>
          CodeLines
            .from(CodeLines.debugHere, frovedisStringProducer.produce(outputName))
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

        case f: FrovedisStringProducer =>
          CodeLines.from(CodeLines.debugHere, f.complete(outputName))
      }

    def validityForEach(idx: String): CodeLines =
      CodeLines.from(s"set_validity($outputName->validityBuffer, $idx, 1);")
  }

  def produceVarChar(
    count: String,
    outputName: String,
    stringProducer: StringProducer
  ): CodeLines = {
    val fp = FilteringProducer(outputName, stringProducer)
    CodeLines.from(
      fp.setup,
      s"""for ( int32_t i = 0; i < $count; i++ ) {""",
      fp.forEach.indented,
      "}",
      fp.complete,
      s"for( int32_t i = 0; i < $count; i++ ) {",
      CodeLines.from(fp.validityForEach("i")).indented,
      "}"
    )
  }
}
