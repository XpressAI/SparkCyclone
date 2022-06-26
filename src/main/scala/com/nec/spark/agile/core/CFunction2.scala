package com.nec.spark.agile.core

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

    final case class PointerPointerPointer(vec: CVector) extends CFunctionArgument {
      override def render: String = s"${vec.veType.cVectorType}*** ${vec.name}"
    }
  }
}

final case class CFunction2(name: String,
                            arguments: Seq[CFunction2.CFunctionArgument],
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
      definition
    )
  }

  def declaration: CodeLines = {
    CodeLines.from(
      s"""extern "C" long ${name} (""",
      arguments.map(arg => s"  ${arg.render}").mkString(",\n"),
      ");",
      ""
    )
  }

  def definition: CodeLines = {
    CodeLines.from(
      s"""extern "C" long ${name} (""",
      arguments.map(arg => s"  ${arg.render}").mkString(",\n"),
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
