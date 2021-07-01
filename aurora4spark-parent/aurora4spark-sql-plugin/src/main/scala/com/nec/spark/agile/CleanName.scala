package com.nec.spark.agile
import java.util.regex.Pattern

final case class CleanName(value: String) {
  override def toString: String = value
}
object CleanName {

  implicit class RichStringClean(string: String) {
    def clean: CleanName = fromString(string)
    def cleanName: CleanName = fromString(string)
  }
  def fromString(value: String): CleanName = CleanName(
    value
      .replaceAll(Pattern.quote("-"), "_minus_")
      .replaceAll(Pattern.quote("+"), "_plus_")
      .replaceAll("[^a-zA-Z_0-9]", "")
  )
}
