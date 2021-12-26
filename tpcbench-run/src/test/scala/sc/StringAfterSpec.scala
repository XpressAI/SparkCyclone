package sc

import org.scalatest.freespec.AnyFreeSpec
import sc.StringUtils.afterStart

final class StringAfterSpec extends AnyFreeSpec {
  "it extracts" in {
    assert(afterStart("Compilation time: 123", "Compilation time: ").contains("123"))
  }
  "it does not extract when not at start" in {
    assert(afterStart("xCompilation time: 123", "Compilation time: ").isEmpty)
  }
  "it does not extract when not there" in {
    assert(afterStart("xCompilation time: 123", "abcdef: ").isEmpty)
  }
}
