package io.sparkcyclone.benchmarks

object StringUtils {
  def afterStart(haystack: String, needle: String): Option[String] =
    if (haystack.startsWith(needle)) Some(haystack.drop(needle.length)) else None
}
