package com.nec.spark.agile

import com.nec.spark.agile.CFunctionGeneration.CExpression

import scala.annotation.tailrec

object FlatToNestedFunction {
  def nest(items: Seq[String], joiner: String): String = {

    @tailrec
    def go(remaining: List[String], joined: String, currentDepth: Int): String = {
      remaining match {
        case Nil => joined + (")" * (currentDepth - 1))
        case last :: Nil =>
          go(Nil, s"${joined}${last}", currentDepth + 1)
        case h :: rest if currentDepth == 0 =>
          go(rest, s"$joiner(${h}", currentDepth + 1)
        case h :: rest =>
          go(rest, s"${joined}, $joiner(${h}, ", currentDepth + 1)
      }
    }

    if (items.length == 2) s"${joiner}(${items(0)}, ${items(1)})" else go(items.toList, "", 0)
  }

  def runWhenNotNull(items: List[CExpression], function: String): CExpression = {
    items match {
      case Nil         => CExpression(cCode = "0", isNotNullCode = Some("0"))
      case item :: Nil => item
      case CExpression(cCode, None) :: CExpression(cCode2, None) :: Nil =>
        CExpression(cCode = s"${function}($cCode, $cCode2)", isNotNullCode = None)
      case CExpression(cCode, None) :: CExpression(cCode2, Some(notNull)) :: Nil =>
        CExpression(
          cCode = s"(${notNull}) ? (${function}($cCode, $cCode2)) : ${cCode}",
          isNotNullCode = None
        )
      case CExpression(cCode, Some(notNull)) :: CExpression(cCode2, None) :: Nil =>
        CExpression(
          cCode = s"(${notNull}) ? (${function}($cCode, $cCode2)) : ${cCode2}",
          isNotNullCode = None
        )
      case CExpression(cCode, Some(notNull)) :: CExpression(cCode2, Some(notNull2)) :: Nil =>
        CExpression(
          cCode =
            s"(${notNull} && ${notNull2}) ? (${function}($cCode, $cCode2)) : (${notNull} ? $cCode : $cCode2)",
          isNotNullCode = Some(s"${notNull} || ${notNull2}")
        )
      case other :: rest =>
        runWhenNotNull(items = other :: runWhenNotNull(rest, function) :: Nil, function = function)
    }
  }

}
