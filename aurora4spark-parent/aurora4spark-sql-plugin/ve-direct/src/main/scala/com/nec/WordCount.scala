package com.nec

import com.nec.CountStringsLibrary.{data_out, unique_position_counter}
import com.nec.aurora.Aurora
import com.sun.jna.{Library, Pointer}
import com.sun.jna.ptr.PointerByReference
import org.bytedeco.javacpp.LongPointer

import java.nio.file.Path
import java.nio.{ByteBuffer, ByteOrder}
import scala.language.higherKinds

object WordCount {

  val SourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/sort-stuff-lib.c"))
    try source.mkString
    finally source.close()
  }

  final case class SomeStrings(strings: String*) {
    def someStrings: Array[String] = strings.toArray
    def stringsByteArray: Array[Byte] = someStrings.flatMap(_.getBytes)
    def someStringByteBuffer: ByteBuffer = {
      val bb = ByteBuffer.allocateDirect(stringsByteArray.length)
      bb.order(ByteOrder.LITTLE_ENDIAN)
      bb.put(stringsByteArray)
      bb.position(0)
      bb
    }
    def arrSize: Int = stringsByteArray.length
    def stringPositions: Array[Int] = someStrings.map(_.length).scanLeft(0)(_ + _).dropRight(1)
    def sbbLen: Int = stringPositions.length * 4

    def stringLengthsBb: ByteBuffer = {
      val bb = ByteBuffer.allocateDirect(stringLengthsBbSize)
      bb.order(ByteOrder.LITTLE_ENDIAN)
      stringLengths.zipWithIndex.foreach { case (v, idx) => bb.putInt(idx * 4, v) }
      bb.position(0)
      bb
    }
    def stringLengths: Array[Int] = someStrings.map(_.length)
    def stringLengthsBbSize: Int = stringLengths.length * 4
    def stringPositionsBB: ByteBuffer = {
      val lim = stringPositions.length * 4
      val bb = ByteBuffer.allocateDirect(lim)
      bb.order(ByteOrder.LITTLE_ENDIAN)
      stringPositions.zipWithIndex.foreach { case (v, idx) =>
        val tgt = idx * 4
        bb.putInt(tgt, v)
      }
      bb.position(0)
      bb
    }

    def expectedWordCount: Map[String, Int] = someStrings
      .groupBy(identity)
      .mapValues(_.length)

    private val count_strings = "count_strings"

    def computex86(libPath: Path): Map[String, Int] = {
      // will abstract this out later
      import scala.collection.JavaConverters._
      val thingy2 =
        new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
      val nl = thingy2.getNativeLibrary
      val fn = nl.getFunction(count_strings)
      println(fn)

      val byteArray = someStrings.flatMap(_.getBytes)
      val bb = ByteBuffer.allocate(byteArray.length)
      bb.put(byteArray)
      bb.position(0)

      val stringPositions = someStrings.map(_.length).scanLeft(0)(_ + _).dropRight(1)
      val dc = new data_out.ByReference()
      fn.invokeInt(
        Array[java.lang.Object](
          bb,
          stringPositions,
          someStrings.map(_.length),
          java.lang.Integer.valueOf(someStrings.length),
          dc
        )
      )

      val counted_strings = dc.logical_total.toInt
      assert(counted_strings == strings.toSet.size)

      val results =
        (0 until counted_strings).map { i =>
          new unique_position_counter(new Pointer(Pointer.nativeValue(dc.data) + i * 8))
        }
      results.map { unique_position_counter =>
        someStrings(unique_position_counter.string_i) -> unique_position_counter.count
      }.toMap
    }

    def computeVE(
      proc: Aurora.veo_proc_handle,
      ctx: Aurora.veo_thr_ctxt,
      libPath: Path
    ): Map[String, Int] = {
      val lib: Long = Aurora.veo_load_library(proc, libPath.toString)
      val our_args = Aurora.veo_args_alloc()
      val lgSize = 24
      val longPointer = new LongPointer(lgSize)
      longPointer.put(0, 0)
      longPointer.put(1, 0)
      longPointer.put(2, 0)
      val strBb = someStringByteBuffer

      def copyBufferToVe(byteBuffer: ByteBuffer): Long = {
        val veInputPointer = new LongPointer(8)
        val size = byteBuffer.capacity()
        Aurora.veo_alloc_mem(proc, veInputPointer, size)
        Aurora.veo_write_mem(
          proc,
          veInputPointer.get(),
          new org.bytedeco.javacpp.Pointer(byteBuffer),
          size
        )
        val inputVePointer = new LongPointer(8)
        inputVePointer.put(veInputPointer.get())
        inputVePointer.get()
      }

      Aurora.veo_args_set_i64(our_args, 0, copyBufferToVe(strBb))
      Aurora.veo_args_set_i64(our_args, 1, copyBufferToVe(stringPositionsBB))
      Aurora.veo_args_set_i64(our_args, 2, copyBufferToVe(stringLengthsBb))
      Aurora.veo_args_set_i32(our_args, 3, someStrings.length)
      Aurora.veo_args_set_stack(our_args, 1, 4, longPointer.asByteBuffer(), lgSize)

      try {
        val req_id = Aurora.veo_call_async_by_name(ctx, lib, count_strings, our_args)
        val fnCallResult = new LongPointer(8)
        try {
          val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
          require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
          val veLocation = longPointer.get(0)
          val counted_strings = longPointer.get(1).toInt
          val resLen = longPointer.get(2).toInt

          val vhTarget = ByteBuffer.allocateDirect(resLen)
          Aurora.veo_read_mem(proc, new org.bytedeco.javacpp.Pointer(vhTarget), veLocation, resLen)
          val resultsPtr = new PointerByReference(
            new Pointer(vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address())
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
      } finally Aurora.veo_args_free(our_args)
    }
  }
}
