import com.nec.VeVars
import org.scalatest.freespec.AnyFreeSpec

final class VeVarsSpec extends AnyFreeSpec {
  "We can detect the different vars" in {
    val linesBefore =
      List("""declare -x ASL_HOME="/opt/nec/ve/nlc/2.2.0"""").mkString("\n", "\n", "\n")
    val linesAfter = List(
      """declare -x ASL_HOME="/opt/nec/ve/nlc/2.2.0"""",
      """declare -x ASL_HOMEs="/test"""",
      """declare -x ASL_HOME="/opt/nec/ve/nlc/2.2.0""""
    ).mkString("\n")
    assert(VeVars.getDiff(linesBefore, linesAfter).get("ASL_HOMEs").contains("/test"))
  }
}
