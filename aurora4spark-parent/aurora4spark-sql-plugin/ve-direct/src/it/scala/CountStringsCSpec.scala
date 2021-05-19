import CountStringsCSpec.CMakeListsTXT
import com.nec.WordCount
import org.apache.commons.io.FileUtils
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.Instant
import scala.sys.process._

object CountStringsCSpec {

  lazy val CMakeListsTXT: Path = Paths
    .get(
      this.getClass
        .getResource("/CMakeLists.txt")
        .toURI
    )
    .toAbsolutePath

}

final class CountStringsCSpec extends AnyFreeSpec {
  "It works" in {
    val targetDir = Paths.get("target", s"c", s"${Instant.now().toEpochMilli}").toAbsolutePath
    if (Files.exists(targetDir)) {
      FileUtils.deleteDirectory(targetDir.toFile)
    }
    Files.createDirectories(targetDir)
    val tgtCl = targetDir.resolve(CMakeListsTXT.getFileName)
    Files.copy(CMakeListsTXT, tgtCl, StandardCopyOption.REPLACE_EXISTING)
    Files.write(targetDir.resolve("sort-stuff-lib.c"), WordCount.SourceCode.getBytes())

    val ss = CountStringsVESpec.Sample
    val result = ss.computex86(buildAndLink(tgtCl))
    info(s"Got: $result")
    assert(result == ss.expectedWordCount)
  }

  private def buildAndLink(targetPath: Path): Path = {
    val os = System.getProperty("os.name").toLowerCase

    os match {
      case _ if os.contains("win") => buildAndLinkWin(targetPath)
      case _ if os.contains("lin") => buildAndLinkLin(targetPath)
      case _                       => buildAndLinkMacos(targetPath)
    }
  }

  private def buildAndLinkWin(targetPath: Path): Path = {
    val cmd = List("C:\\Program Files\\CMake\\bin\\cmake", "-A", "x64", targetPath.toString)
    val cmd2 =
      List("C:\\Program Files\\CMake\\bin\\cmake", "--build", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    targetPath.getParent.resolve("Debug").resolve("sortstuff.dll")
  }
  private def buildAndLinkMacos(targetPath: Path): Path = {
    val cmd = List("cmake", targetPath.toString)
    val cmd2 = List("make", "-C", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    targetPath.getParent.resolve("libsortstuff.dylib")
  }
  private def buildAndLinkLin(targetPath: Path): Path = {
    val cmd = List("cmake", targetPath.toString)
    val cmd2 = List("make", "-C", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    targetPath.getParent.resolve("libsortstuff.so")
  }
}
