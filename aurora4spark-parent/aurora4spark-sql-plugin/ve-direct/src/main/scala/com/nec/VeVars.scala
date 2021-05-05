package com.nec

import java.nio.file.{Path, Paths}

object VeVars {
  val DefaultSource = Paths.get("/opt/nec/ve/nlc/2.2.0/bin/nlcvars.sh")

  object DeclareLine {
    def unapply(input: String): Option[(String, String)] = {
      val declareX = "declare -x "
      if (input.startsWith(declareX)) {
        val ni = input.drop(declareX.length)
        val equalsPos = ni.indexOf('=')
        if (equalsPos < 0) None
        else
          Some {
            ni.take(equalsPos) -> ni.trim.drop(equalsPos).drop(2).dropRight(1)
          }
      } else None
    }
  }
  def getDecls(string: String): Map[String, String] = {
    string
      .split("\r?\n")
      .collect { case DeclareLine(key, value) =>
        key -> value
      }
      .toMap
  }

  def getDiff(before: String, after: String): Map[String, String] = {
    val beforeMap = getDecls(before)
    val afterMap = getDecls(after)

    afterMap.filter { case (key, value) =>
      !beforeMap.get(key).contains(value)
    }
  }

  def checkVars(sourceBash: Path = DefaultSource): Map[String, String] = {
    import scala.sys.process._
    val before = List("bash", "-c", "export").!!
    val after = List("bash", "-c", s"source ${sourceBash} && export").!!
    getDiff(before, after)
  }
}
