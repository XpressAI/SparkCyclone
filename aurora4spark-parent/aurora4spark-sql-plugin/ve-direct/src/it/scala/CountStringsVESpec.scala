import CountStringsLibrary.unique_position_counter
import com.nec.aurora.Aurora
import com.sun.jna.Pointer
import org.scalatest.freespec.AnyFreeSpec

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.sys.process._
import CountStringsVESpec._
import com.nec.VeCompiler
import com.sun.jna.ptr.PointerByReference
import org.bytedeco.javacpp.LongPointer
import java.nio.LongBuffer
import java.nio.ByteOrder
object CountStringsVESpec {

  lazy val LibSource: String = new String(Files.readAllBytes(CountStringsCSpec.SortStuffLibC))

  final case class SomeStrings(strings: String*) {
    def someStrings: Array[String] = strings.toArray
    def stringsByteArray: Array[Byte] = someStrings.flatMap(_.getBytes)
    def someStringByteBuffer: ByteBuffer = {
      val bb = ByteBuffer.allocate(stringsByteArray.length)
      bb.put(stringsByteArray)
      bb.position(0)
      bb
    }
    def arrSize: Int = stringsByteArray.length
    def stringPositions: Array[Int] = someStrings.map(_.length).scanLeft(0)(_ + _).dropRight(1)
    def sbbLen: Int = stringPositions.length * 4

    def stringLengthsBb: ByteBuffer = {
      val bb = ByteBuffer.allocate(stringLengthsBbSize)
      stringLengths.zipWithIndex.foreach { case (v, idx) => bb.putInt(idx * 4, v) }
      bb.position(0)
      bb
    }
    def stringLengths: Array[Int] = someStrings.map(_.length)
    def stringLengthsBbSize: Int = {
      stringLengths.length * 4
    }
    def stringPositionsBB: ByteBuffer = {
        val lim = stringPositions.length * 4
      val bb = ByteBuffer.allocate(lim)
      stringPositions.zipWithIndex.foreach { case (v, idx) => 
        val tgt = idx * 4
        println(s"Trying to put to ${tgt}; ${lim}")
        bb.putInt(tgt, v)
    
    }
      bb.position(0)
      bb
    }
    def expectedWordCount: Map[String, Int] = someStrings
      .groupBy(identity)
      .mapValues(_.length)
  }

  val Sample = SomeStrings("hello", "dear", "world", "of", "hello", "of", "hello")
}

final class CountStringsVESpec extends AnyFreeSpec {
  "It works" ignore {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeCompiler("wc", veBuildPath).compile_c(LibSource)
    import Sample._
    val proc = Aurora.veo_proc_create(0)
    val wordCount =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
//        int count_strings(void* strings, int* string_positions, int* string_lengths, int num_strings, void** rets, int* counted) {

          val our_args = Aurora.veo_args_alloc()
          val longPointer = new LongPointer(8)
          val strBb = someStringByteBuffer
          Aurora.veo_args_set_stack(our_args, 0, 0, strBb, arrSize)
          Aurora.veo_args_set_stack(our_args, 1, 0, stringPositionsBB, sbbLen)
          Aurora.veo_args_set_stack(our_args, 2, 0, stringLengthsBb, stringLengthsBbSize)
          Aurora.veo_args_set_i32(our_args, 3, someStrings.length)
          Aurora.veo_args_set_stack(our_args, 4, 2, longPointer.asByteBuffer(), 8)

          

          val resultsPtr = new PointerByReference()

          /** Call */
          try {
            val req_id = Aurora.veo_call_async_by_name(ctx, lib, "count_strings_ve", our_args)
            val lengthOfItemsPointer = new LongPointer(8)
            try {
              val counted_strings = Aurora.veo_call_wait_result(ctx, req_id, lengthOfItemsPointer)

              val veLocation = lengthOfItemsPointer.get()
              val vhTarget = ByteBuffer.allocateDirect(counted_strings * 8)
              Aurora.veo_read_mem(
                proc,
                new org.bytedeco.javacpp.Pointer(vhTarget),
                veLocation,
                counted_strings * 8
              )
              val results = (0 until counted_strings)
                .map(i =>
                  new unique_position_counter(
                    new Pointer(Pointer.nativeValue(resultsPtr.getValue) + i * 8)
                  )
                )
                .map { unique_position_counter =>
                  someStrings(unique_position_counter.string_i) -> unique_position_counter.count
                }
                .toMap
              results
            } finally longPointer.close()
          } finally {
            Aurora.veo_args_free(our_args)
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    info(s"Got: $wordCount")
    assert(wordCount == expectedWordCount)
  }

  "It can read some VE-allocated memory" in {
    val veBuildPath = Paths.get("target", "ve", s"${Instant.now().toEpochMilli}").toAbsolutePath
    val libPath = VeCompiler("wc", veBuildPath).compile_c(LibSource)
    import Sample._
    val proc = Aurora.veo_proc_create(0)
    val wordCount =
      try {
        val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
        try {
          val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
          val our_args = Aurora.veo_args_alloc()
          val longPointer = new LongPointer(1)
          longPointer.put(2)
          longPointer.position(0)
          Aurora.veo_args_set_stack(our_args, 2, 0, longPointer.asByteBuffer(), 8)
          try {
            val req_id = Aurora.veo_call_async_by_name(ctx, lib, "get_veo_data", our_args)
            val rl = new LongPointer(1)
            val counted_strings = Aurora.veo_call_wait_result(ctx, req_id, rl)
            longPointer.position(0)
            println(s"lng => ${longPointer.get()}")
            println(s"lng => ${longPointer.get()}")
            println(s"Result = ${rl.get()}")
            val vhTarget = ByteBuffer.allocateDirect(2 * 4)
            val r2 = Aurora.veo_read_mem(
                proc,
                new org.bytedeco.javacpp.Pointer(vhTarget),
                longPointer.get(),
                8
            )
            println(s"R2 = $r2")
            vhTarget.order(ByteOrder.LITTLE_ENDIAN)
            println(vhTarget.getInt(0))
            println(vhTarget.getInt(4))
          } finally {
            Aurora.veo_args_free(our_args)
          }
        } finally Aurora.veo_context_close(ctx)
      } finally Aurora.veo_proc_destroy(proc)

    info(s"Got: $wordCount")
    assert(wordCount == expectedWordCount)
  }

}
