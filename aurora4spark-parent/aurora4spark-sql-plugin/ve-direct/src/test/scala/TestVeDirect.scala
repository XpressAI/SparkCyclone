import org.scalatest.freespec.AnyFreeSpec
import com.nec.aurora.Aurora
final class TestVeDirect extends AnyFreeSpec {
  "It works" in {
    val proc = Aurora.veo_proc_create(0)
    try println(s"Created proc = ${proc}")
    finally Aurora.veo_proc_destroy(proc)
  }
}
