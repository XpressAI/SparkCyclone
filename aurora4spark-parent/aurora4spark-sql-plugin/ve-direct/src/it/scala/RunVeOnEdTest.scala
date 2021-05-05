import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.Paths

final class RunVeOnEdTest extends AnyFreeSpec {
  "it works" in {
    val jarResource = this.getClass.getResource("/ve-direct-assembly-0.1.0-SNAPSHOT.jar")
    assert(jarResource != null)
    import scala.sys.process._
    val uploadResult =
      List("scp", Paths.get(jarResource.toURI).toAbsolutePath.toString, "ed:vd.jar").!!
    info(uploadResult)

    def runResult =
      List(
        "ssh",
        "ed",
        s"bash -c 'source /opt/nec/ve/nlc/2.2.0/bin/nlcvars.sh && java -jar vd.jar'"
      ).!!
    info(runResult)
  }
}
