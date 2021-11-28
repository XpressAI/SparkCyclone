package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.CVector

object CFunction2 {
  sealed trait CFunctionArgument {
    def renderValue: String
  }
  object CFunctionArgument {
    final case class Raw(value: String) extends CFunctionArgument {
      override def renderValue: String = value
    }
    final case class Pointer(cVector: CVector) extends CFunctionArgument {
      override def renderValue: String = s"${cVector}* ${cVector.veType.cVectorType}"
    }
    final case class PointerPointer(cVector: CVector) extends CFunctionArgument {
      override def renderValue: String = s"${cVector}** ${cVector.veType.cVectorType}"
    }
  }
}
final case class CFunction2(arguments: List[CFunctionArgument], body: CodeLines) {
  def toCodeLines(name: String): CodeLines = CodeLines.from(
    s"""extern "C" long $name (""",
    arguments.map(_.renderValue).map(arg => s"  $arg").mkString(",\n"),
    ") {",
    CodeLines.from(body, "return 0;").indented,
    "}"
  )
}
