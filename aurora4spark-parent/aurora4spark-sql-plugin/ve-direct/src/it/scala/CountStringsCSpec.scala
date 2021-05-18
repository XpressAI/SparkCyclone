import CountStringsCSpec.{CMakeListsTXT, SortStuffC, SortStuffLibC}
import CountStringsLibrary.unique_position_counter
import com.sun.jna.ptr.PointerByReference
import com.sun.jna.{Library, Native, Pointer}
import com.sun.jna.win32.W32APIOptions
import org.apache.commons.io.FileUtils
import org.scalatest.freespec.AnyFreeSpec

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.Instant
import java.util
import scala.sys.process._

object CountStringsCSpec {

  lazy val SortStuffC: Path = Paths
    .get(
      this.getClass
        .getResource("/sort-stuff.c")
        .toURI
    )
    .toAbsolutePath

  lazy val SortStuffLibC: Path = Paths
    .get(
      this.getClass
        .getResource("/sort-stuff-lib.c")
        .toURI
    )
    .toAbsolutePath

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
    Files.copy(
      SortStuffC,
      targetDir.resolve(SortStuffC.getFileName),
      StandardCopyOption.REPLACE_EXISTING
    )
    Files.copy(
      SortStuffLibC,
      targetDir.resolve(SortStuffLibC.getFileName),
      StandardCopyOption.REPLACE_EXISTING
    )

    val thingy = buildAndLink(tgtCl)

    assert(thingy.add(1, 2) == 3)

    assert(thingy.add_nums(Array(1, 2, 4), 3) == 7)

    val resultsPtr = new PointerByReference()
    val someStrings = Array("hello", "dear", "world", "of", "hello", "of", "hello")
    val byteArray = someStrings.flatMap(_.getBytes)
    val bb = ByteBuffer.allocate(byteArray.length)
    bb.put(byteArray)
    bb.position(0)

    val stringPositions = someStrings.map(_.length).scanLeft(0)(_ + _).dropRight(1)
    val counted_strings = thingy.count_strings(
      bb,
      stringPositions,
      someStrings.map(_.length),
      someStrings.length,
      resultsPtr
    )

    assert(counted_strings == 4)

    val results = (0 until counted_strings).map(i =>
      new unique_position_counter(new Pointer(Pointer.nativeValue(resultsPtr.getValue) + i * 8))
    )

    val expected_results: Map[String, Int] = someStrings
      .groupBy(identity)
      .mapValues(_.length)

    val result = results.map { unique_position_counter =>
      someStrings(unique_position_counter.string_i) -> unique_position_counter.count
    }.toMap

    info(s"Got: $result")
    assert(result == expected_results)
  }

  private def buildAndLink(targetPath: Path): CountStringsLibrary = {
    val os = System.getProperty("os.name").toLowerCase

    os match {
      case _ if os.contains("win") => buildAndLinkWin(targetPath)
      case _ if os.contains("lin") => buildAndLinkLin(targetPath)
      case _ => buildAndLinkMacos(targetPath)
    }
  }

  private def buildAndLinkWin(targetPath: Path): CountStringsLibrary = {
    val cmd = List("C:\\Program Files\\CMake\\bin\\cmake", "-A", "x64", targetPath.toString)
    val cmd2 = List("C:\\Program Files\\CMake\\bin\\cmake", "--build", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    val libPath = targetPath.getParent.resolve("Debug").resolve("sortstuff.dll")
    val opts = new util.HashMap[String, AnyRef](W32APIOptions.DEFAULT_OPTIONS)
    opts.put(Library.OPTION_CLASSLOADER, null)
    Native.loadLibrary(libPath.toString, classOf[CountStringsLibrary], opts)
  }

  private def buildAndLinkMacos(targetPath: Path): CountStringsLibrary = {
    val cmd = List("cmake", targetPath.toString)
    val cmd2 = List("make", "-C", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    val libPath = targetPath.getParent.resolve("libsortstuff.dylib")
    Native.loadLibrary(libPath.toString, classOf[CountStringsLibrary])
  }
  private def buildAndLinkLin(targetPath: Path): CountStringsLibrary = {
    val cmd = List("cmake", targetPath.toString)
    val cmd2 = List("make", "-C", targetPath.getParent.toString)
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
    val libPath = targetPath.getParent.resolve("libsortstuff.so")
    Native.loadLibrary(libPath.toString, classOf[CountStringsLibrary])
  }
}
