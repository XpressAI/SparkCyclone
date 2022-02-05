package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CodeStructure.CodeSection

final case class CodeStructure(sections: List[CodeSection])

object CodeStructure {

  def combine(list: List[CodeStructure]): CodeLines = {
    CodeLines.from(
      list
        .flatMap(_.sections)
        .sortBy {
          case CodeSection.Header(_)    => 1
          case CodeSection.NonHeader(_) => 2
        }
        .distinct
        .map(_.codeLines)
    )
  }

  val empty: CodeStructure = CodeStructure(sections = Nil)
  sealed trait CodeSection {
    def codeLines: CodeLines
  }
  object CodeSection {
    final case class Header(codeLines: CodeLines) extends CodeSection

    final case class NonHeader(codeLines: CodeLines) extends CodeSection
  }

  /** General code stucture parser: assumes that headers at the top and the rest of things happen after headers finish. */

  def from(codeLines: CodeLines): CodeStructure = {
    var lookingForHeader = true
    val foundParts = scala.collection.mutable.Buffer.empty[CodeSection]
    var remaining = codeLines.lines.dropWhile(_.trim.isEmpty)

    while (remaining.nonEmpty) {
      if (lookingForHeader) {
        if (remaining.head.startsWith("#if")) {
          val remainingBits = remaining.dropWhile(str => !str.startsWith("#endif"))
          val headerBits = remaining.takeWhile(str => !str.startsWith("#endif"))
          remaining = remainingBits.tail
          foundParts.append(CodeSection.Header(CodeLines.from(headerBits, remainingBits.take(1))))
        } else if (remaining.head.startsWith("#")) {
          foundParts.append(CodeSection.Header(CodeLines.from(remaining.head)))
          remaining = remaining.drop(1)
        } else {
          lookingForHeader = false
        }
      } else {
        foundParts.append(CodeSection.NonHeader(CodeLines.from(remaining)))
        remaining = Nil
      }
    }

    CodeStructure(sections = foundParts.toList)
  }
}
