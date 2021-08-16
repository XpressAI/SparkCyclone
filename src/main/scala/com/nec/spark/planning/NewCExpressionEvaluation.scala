package com.nec.spark.planning
import com.nec.spark.agile.CExpressionEvaluation
import com.nec.spark.agile.CExpressionEvaluation.RichListStr
import org.apache.spark.sql.catalyst.expressions.Attribute

object NewCExpressionEvaluation {
  def evaluate(
    fName: String,
    output: Seq[Attribute],
    tgt: String,
    beginIndex: Int,
    endIndex: Int
  ): CExpressionEvaluation.CodeLines = {
    List[List[String]](
      List("#include \"substr.cpp\""),
      List(
        s"""extern "C" long ${fName}(non_null_varchar_vector* input_strings, non_null_varchar_vector* output_strings) {""",
        s"return ve_substr(input_strings, output_strings, $beginIndex, $endIndex);",
        "}"
      )
    ).flatten.codeLines
  }
}
