package com.nec

import com.nec.CountStringsLibrary.unique_position_counter
import com.nec.aurora.Aurora
import com.sun.jna.{Native, Pointer}
import com.sun.jna.ptr.PointerByReference
import org.bytedeco.javacpp.LongPointer

import java.nio.file.Path
import java.nio.{ByteBuffer, ByteOrder}

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

    def computex86(libPath: Path): Map[String, Int] = {
      // will abstract this out later
      val thingy = Native.loadLibrary(libPath.toString, classOf[CountStringsLibrary])

      val resultsPtr = new PointerByReference()
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

      assert(counted_strings == strings.toSet.size)

      val results = (0 until counted_strings).map(i =>
        new unique_position_counter(new Pointer(Pointer.nativeValue(resultsPtr.getValue) + i * 8))
      )
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
      val longPointer = new LongPointer(8)
      val strBb = someStringByteBuffer
      val veInputPointer = new LongPointer(8)
      Aurora.veo_alloc_mem(proc, veInputPointer, stringsByteArray.length)
      Aurora.veo_write_mem(
        proc,
        veInputPointer.get(),
        new org.bytedeco.javacpp.Pointer(strBb),
        stringsByteArray.length
      )
      val inputVePointer = new LongPointer(8)
      inputVePointer.put(veInputPointer.get())

      Aurora.veo_args_set_i64(our_args, 0, veInputPointer.get())
      Aurora.veo_args_set_stack(our_args, 0, 1, stringPositionsBB, sbbLen)
      Aurora.veo_args_set_stack(our_args, 0, 2, stringLengthsBb, stringLengthsBbSize)
      Aurora.veo_args_set_i32(our_args, 3, someStrings.length)
      Aurora.veo_args_set_stack(our_args, 2, 4, longPointer.asByteBuffer(), 8)

      /** Call */
      try {
        val req_id = Aurora.veo_call_async_by_name(ctx, lib, "count_strings", our_args)
        val lengthOfItemsPointer = new LongPointer(1)
        try {
          val callRes = Aurora.veo_call_wait_result(ctx, req_id, lengthOfItemsPointer)
          require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
          val counted_strings = lengthOfItemsPointer.get().toInt

          val veLocation = longPointer.get()
          val resLen = counted_strings * 8
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
