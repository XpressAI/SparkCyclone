package io.sparkcyclone.spark.agile.core

import scala.annotation.tailrec
import sourcecode.{FullName, Line}

object CodeLines {
  def parse(source: String): CodeLines = {
    CodeLines.from(source.split("\\r?\\n").toList)
  }

  def empty: CodeLines = CodeLines(Seq.empty[String])

  def commentHere(comments: String*)(implicit name: FullName, line: Line): CodeLines = {
    CodeLines.from(
      comments.map(w => CodeLines.from(s"// $w")),
      s"// ${name.value} (#${line.value})"
    )
  }

  def from(str: CodeLines*): CodeLines = {
    CodeLines(lines = str.flatMap(_.lines).toList)
  }

  def ifStatement(condition: String)(sub: => CodeLines): CodeLines = {
    CodeLines.from(s"if ($condition) { ", sub.indented, "}")
  }

  def ifElseStatement(condition: String)(sub1: => CodeLines)(sub2: CodeLines): CodeLines = {
    CodeLines.from(s"if ($condition) { ", sub1.indented, "} else {", sub2.indented, "}")
  }

  def forLoop(counter: String, until: String)(sub: => CodeLines): CodeLines = {
    CodeLines.from(
      s"for (auto $counter = 0; $counter < $until; $counter++) {",
      sub.indented,
      "}"
    )
  }

  def scoped(label: String)(sub: => CodeLines): CodeLines = {
    CodeLines.from(s"{ // CODE BLOCK: ${label}", "", sub.indented, "}", "")
  }

  implicit def toCodeLines1(code: String): CodeLines = CodeLines(Seq(code))

  implicit def toCodeLines2(code: Seq[String]): CodeLines = CodeLines(code)

  implicit def toCodeLines3(code: Seq[CodeLines]): CodeLines = CodeLines(code.flatMap(_.lines))

  implicit class SeqStringToCodeLines(lines: Seq[String]) {
    def toCodeLines: CodeLines = CodeLines(lines)
  }
}

final case class CodeLines(lines: Seq[String]) {
  def ++ (other: CodeLines): CodeLines = {
    CodeLines(lines ++ Seq("") ++ other.lines)
  }

  def scoped(label: String): CodeLines = {
    CodeLines.scoped(label)(lines)
  }

  def indented: CodeLines = {
    CodeLines(lines = lines.map(line => s"  $line"))
  }

  def cCode: String = {
    lines.mkString("\n", "\n", "\n")
  }

  def append(codeLines: CodeLines*): CodeLines = {
    CodeLines(lines ++ codeLines.flatMap(_.lines))
  }

  private[core] def shortenLines(lines: List[String]): List[String] = {
    @tailrec
    def rec(charsSoFar: Int, remaining: List[String], linesSoFar: List[String]): List[String] = {
      remaining match {
        case Nil => linesSoFar
        case one :: rest if one.length + charsSoFar > 100 =>
          linesSoFar ++ List("...")
        case one :: rest =>
          rec(charsSoFar + one.length, rest, linesSoFar ++ List(s"  ${one}"))
      }
    }

    rec(0, lines, Nil)
  }

  override def toString: String = {
    (List(s"CodeLines {") ++ shortenLines(lines.toList) ++ List("}")).mkString("\n")
  }
}
