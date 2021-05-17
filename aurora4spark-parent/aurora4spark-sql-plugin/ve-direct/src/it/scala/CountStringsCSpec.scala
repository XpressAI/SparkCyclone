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
    import scala.sys.process._
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
    val cmd = List("C:\\Program Files\\CMake\\bin\\cmake", "-A", "x64", tgtCl.toString)
    val cmd2 = List("C:\\Program Files\\CMake\\bin\\cmake", "--build", tgtCl.getParent.toString)
    val appPath = tgtCl.getParent.resolve("Debug").resolve("HelloWorld.exe")
    val libPath = tgtCl.getParent.resolve("Debug").resolve("sortstuff.dll")
    assert(cmd.! == 0)
    assert(cmd2.! == 0)
//    val appOutput = List(appPath.toString).!!
//    assert(appOutput == "10 20 25 40 90 100 \r\n")

    val opts = new util.HashMap[String, AnyRef](W32APIOptions.DEFAULT_OPTIONS)
    opts.put(Library.OPTION_CLASSLOADER, null)
    val thingy = Native.loadLibrary(libPath.toString, classOf[CountStringsLibrary], opts)

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
}
