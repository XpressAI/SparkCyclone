package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines

trait StringProducer {
  def produceTo(tempStringName: String, lenName: String): CodeLines
}

object StringProducer {
  def produceVarChar(outputName: String, stringProducer: StringProducer): CodeLines = {
    val tmpString = s"${outputName}_tmp";
    val tmpOffsets = s"${outputName}_tmp_offsets";
    val tmpCurrentOffset = s"${outputName}_tmp_current_offset";
    val tmpCount = s"${outputName}_tmp_count";
    CodeLines.from(
      s"""std::string ${tmpString}("");""",
      s"""std::vector<int32_t> ${tmpOffsets};""",
      s"""int32_t ${tmpCurrentOffset} = 0;""",
      s"int ${tmpCount} = 0;",
      """for ( int32_t i = 0; i < input_0->count; i++ ) {""",
      CodeLines
        .from(
          "int len = 0;",
          stringProducer.produceTo(s"${tmpString}", "len"),
          s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
          s"""${tmpCurrentOffset} += len;""",
          s"${tmpCount}++;"
        )
        .indented,
      "}",
      s"""${tmpOffsets}.push_back(${tmpCurrentOffset});""",
      s"""${outputName}->count = ${tmpCount};""",
      s"""${outputName}->size = ${tmpCurrentOffset};""",
      s"""${outputName}->data = (char*)malloc(${outputName}->size);""",
      s"""memcpy(${outputName}->data, ${tmpString}.data(), ${outputName}->size);""",
      s"""${outputName}->offsets = (int32_t*)malloc(sizeof(int32_t) * (${outputName}->count + 1));""",
      s"""memcpy(${outputName}->offsets, ${tmpOffsets}.data(), sizeof(int32_t) * (${outputName}->count + 1));""",
      s"${outputName}->validityBuffer = (unsigned char *) malloc(input_0->count);",
      s"for( int32_t i = 0; i < input_0->count; i++ ) {",
      CodeLines.from(s"set_validity(${outputName}->validityBuffer, i, 1);").indented,
      "}"
    )
  }

}
