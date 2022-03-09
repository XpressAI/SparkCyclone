package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.CVector

object CFunction2 {
  sealed trait IncludeHeader {
    def name: String
  }

  case class StandardHeader(name: String) extends IncludeHeader {
    override def toString: String = s"#include <${name}>"
  }

  case class UserHeader(name: String) extends IncludeHeader {
    override def toString: String = s"""#include "${name}""""
  }

  val DefaultHeaders: Set[IncludeHeader] = {
    Set(
      UserHeader("cyclone/cyclone.hpp"),
      StandardHeader("math.h"),
      StandardHeader("stddef.h"),
      StandardHeader("vector"),
      StandardHeader("string")
    )
  }

  sealed trait CFunctionArgument {
    def render: String
  }

  object CFunctionArgument {
    final case class Raw(value: String) extends CFunctionArgument {
      override def render: String = value
    }

    final case class Pointer(vec: CVector) extends CFunctionArgument {
      override def render: String = s"${vec.veType.cVectorType}* ${vec.name}"
    }

    final case class PointerPointer(vec: CVector) extends CFunctionArgument {
      override def render: String = s"${vec.veType.cVectorType}** ${vec.name}"
    }
  }
}

final case class CFunction2(name: String,
                            arguments: List[CFunctionArgument],
                            body: CodeLines,
                            additionalHeaders: Set[CFunction2.IncludeHeader] = Set.empty) {
  def toCodeLinesWithHeaders: CodeLines = {
    val headers = (additionalHeaders ++ CFunction2.DefaultHeaders).toSeq.sortBy { header =>
      (
        header.isInstanceOf[CFunction2.StandardHeader],
        !header.name.endsWith(".h"),
        header.name
      )
    }

    CodeLines.from(
      headers.map(_.toString).mkString("\n"),
      "",
      toCodeLines
    )
  }

  def toCodeLines: CodeLines = {
    CodeLines.from(
      s"""extern "C" long ${name} (""",
      arguments.map(_.render).map(arg => s"  ${arg}").mkString(",\n"),
      ") {",
      CodeLines.from(
        body,
        "",
        "return 0;"
      ).indented,
      "}",
      ""
    )
  }
}
