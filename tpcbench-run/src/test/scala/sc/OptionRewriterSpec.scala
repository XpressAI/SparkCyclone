package sc

import com.eed3si9n.expecty.Expecty.expect
import org.scalatest.freespec.AnyFreeSpec

final class OptionRewriterSpec extends AnyFreeSpec {
  "it works" in {
    assert(RunOptions.default.rewriteArgs("--scale=19").get.scale == "19")
  }
  "it can also add config options" in {
    assert(
      RunOptions.default
        .rewriteArgs("--extra=--conf")
        .flatMap(_.rewriteArgs("--extra=x=y"))
        .get
        .extras
        .contains("--conf x=y")
    )
  }
  "We can add --conf directly too" in {
    assert(
      RunOptions.default.enhanceWith(List("z", "--conf", "test")).extras.contains("--conf test")
    )
  }
  "We can add env vars" in {
    val r =
      RunOptions.default.enhanceWithEnv(Map("INPUT_query" -> "2", "INPUT_extra" -> "--conf test"))
    expect(r.extras.get == "--conf test", r.queryNo == 2)
  }
}
