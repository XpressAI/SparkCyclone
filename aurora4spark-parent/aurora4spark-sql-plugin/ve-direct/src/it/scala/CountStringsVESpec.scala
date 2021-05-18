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

  lazy val LibSource: String = new String(Files.readAllBytes(CountStringsCSpec.SortStuffLibC))

}

final class CountStringsVESpec extends AnyFreeSpec {
  "It works" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VECompiler(veBuildPath).compile_c(LibSource)
    val proc = Aurora.veo_proc_create(0)
    try {
      val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
      try {
        val lib: Long = Aurora.veo_load_library(proc, libPath)
        int count_strings(void* strings, int* string_positions, int* string_lengths, int num_strings, void** rets, int* counted) {

        val our_args = Aurora.veo_args_alloc()

        Aurora.veo_args_set_stack(
            our_args,
            0,
            0,
            new LocationPointer(stringsMemoryAddress, count).asByteBuffer(),
            8 * count
        )
    Aurora.veo_args_set_i64(our_args, 1, count)

    /** Call */
    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "sum", our_args)
      val longPointer = new LongPointer(8)
      try {
        Aurora.veo_call_wait_result(ctx, req_id, longPointer)
        longPointer.asByteBuffer().getDouble(0)
      } finally longPointer.close()
    } finally {
      Aurora.veo_args_free(our_args)
    }
  }
}

        val vej = new VeJavaContext(ctx, lib)
        println(SumSimple.sum_doubles(vej, List(1, 2, 3, 4)))
        println(AvgSimple.avg_doubles(vej, List(1, 2, 3, 10)))
        val multiColumnSumResult = SumMultipleColumns.sum_multiple_doubles(
          vej,
          List(List(1, 2, 3), List(2, 3, 4), List(5, 4, 3), List(10, 10, 10))
        )
        println(multiColumnSumResult)
        assert(multiColumnSumResult == List(6.0, 9.0, 12.0, 30.0))
        val multiColumnAvgResult = AvgMultipleColumns.avg_multiple_doubles(
          vej,
          List(List(5, 10, 15), List(3, 27, 30), List(100, 200, 300), List(1000, 2000, 3000))
        )
        println(multiColumnAvgResult)
        assert(multiColumnAvgResult == List(10.0, 20.0, 200.0, 2000.0))
        println(
          SumPairwise.pairwise_sum_doubles(vej, List[(Double, Double)]((1, 1), (1, 2), (2, 9)))
        )

      } finally Aurora.veo_context_close(ctx)
    } finally Aurora.veo_proc_destroy(proc)
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

}
