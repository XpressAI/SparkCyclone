package com.nec

import com.nec.CountStringsLibrary.{non_null_int_vector, varchar_vector}
import com.nec.aurora.Aurora
import com.sun.jna.{Library, Pointer}
import org.apache.arrow.memory.{ArrowBuf, RootAllocator}
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.{BitVectorHelper, IntVector, VarCharVector}
import org.bytedeco.javacpp.{BytePointer, LongPointer}
import sun.nio.ch.DirectBuffer

import java.nio.file.Path
import java.nio.ByteBuffer
import scala.language.higherKinds

object WordCount {

  val count_strings = "count_strings"
  val SourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/sort-stuff-lib.c"))
    try source.mkString
    finally source.close()
  }

  def copyBufferToVe(proc: Aurora.veo_proc_handle, byteBuffer: ByteBuffer): Long = {
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

  def wordCountArrowVE(
    proc: Aurora.veo_proc_handle,
    ctx: Aurora.veo_thr_ctxt,
    lib: Long,
    varCharVector: VarCharVector
  ): Map[String, Long] = {
    val our_args = Aurora.veo_args_alloc()
    try {
      with_veo_varchar_vector(proc, varCharVector) { varchar_vector =>
        val counted_string_ids = new non_null_int_vector()
        val counted_string_frequencies = new non_null_int_vector()
        Aurora.veo_args_set_stack(
          our_args,
          0,
          0,
          new BytePointer(new LocationPointer(Pointer.nativeValue(varchar_vector.getPointer), 24)),
          24
        )
        Aurora.veo_args_set_stack(
          our_args,
          1,
          1,
          new BytePointer(
            new LocationPointer(Pointer.nativeValue(counted_string_ids.getPointer), 24)
          ),
          24
        )
        Aurora.veo_args_set_stack(
          our_args,
          1,
          2,
          new BytePointer(
            new LocationPointer(Pointer.nativeValue(counted_string_frequencies.getPointer), 24)
          ),
          24
        )
        val req_id = Aurora.veo_call_async_by_name(ctx, lib, count_strings, our_args)
        val fnCallResult = new LongPointer(8)
        val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
        require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
        veo_read_non_null_int_vector(proc, counted_string_ids)
        veo_read_non_null_int_vector(proc, counted_string_frequencies)
        read_word_count(varCharVector, counted_string_ids, counted_string_frequencies)
          .mapValues(_.toLong)
      }
    } finally Aurora.veo_args_free(our_args)
  }

  final case class SomeStrings(strings: String*) {
    def expectedWordCount: Map[String, Int] = strings
      .groupBy(identity)
      .mapValues(_.length)
  }

  def c_varchar_vector(varCharVector: VarCharVector): varchar_vector = {
    val vc = new varchar_vector()
    vc.data = new Pointer(
      varCharVector.getDataBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    )
    vc.offsets = new Pointer(
      varCharVector.getOffsetBuffer.nioBuffer().asInstanceOf[DirectBuffer].address()
    )
    vc.count = varCharVector.getValueCount
    vc
  }

  def with_veo_varchar_vector[T](proc: Aurora.veo_proc_handle, varCharVector: VarCharVector)(
    f: varchar_vector => T
  ): T = {
    val vc = new varchar_vector()
    vc.data = new Pointer(copyBufferToVe(proc, varCharVector.getDataBuffer.nioBuffer()))
    vc.offsets = new Pointer(copyBufferToVe(proc, varCharVector.getOffsetBuffer.nioBuffer()))
    vc.count = varCharVector.getValueCount
    try f(vc)
    finally {
      Aurora.veo_free_mem(proc, Pointer.nativeValue(vc.data))
      Aurora.veo_free_mem(proc, Pointer.nativeValue(vc.offsets))
    }
  }

  /** Take a vec, and rewrite the pointer to our local so we can read it */
  /** Todo consider to deallocate from VE! unless we pass it onward */
  def veo_read_non_null_int_vector(proc: Aurora.veo_proc_handle, vec: non_null_int_vector): Unit = {
    val resLen = vec.byteSize().toInt
    val vhTarget = ByteBuffer.allocateDirect(resLen)
    Aurora.veo_read_mem(
      proc,
      new org.bytedeco.javacpp.Pointer(vhTarget),
      Pointer.nativeValue(vec.data),
      resLen
    )
    vec.data = new Pointer(vhTarget.asInstanceOf[sun.nio.ch.DirectBuffer].address())
  }

  def wordCountArrowCC(libPath: Path, varCharVector: VarCharVector): Map[String, Int] = {
    // will abstract this out later
    import scala.collection.JavaConverters._
    val nativeLibraryHandler =
      new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
    val nl = nativeLibraryHandler.getNativeLibrary
    val fn = nl.getFunction(count_strings)
    val counted_string_ids = new non_null_int_vector()
    val counted_string_frequencies = new non_null_int_vector()
    fn.invokeInt(
      Array[java.lang.Object](
        c_varchar_vector(varCharVector),
        counted_string_ids,
        counted_string_frequencies
      )
    )
    read_word_count(varCharVector, counted_string_ids, counted_string_frequencies)

  }

  def read_word_count(
    varCharVector: VarCharVector,
    counted_string_ids: non_null_int_vector,
    counted_string_frequencies: non_null_int_vector
  ): Map[String, Int] = {
    val counted_strings = counted_string_ids.count.toInt
    val ra = new RootAllocator()
    val res = ra.newReservation()
    res.reserve(counted_strings)
    val string_ids_vector = new IntVector("id", ra)
    non_null_int_vector_to_intVector(counted_string_ids, string_ids_vector, ra)
    val string_frequencies_vector = new IntVector("id", ra)
    non_null_int_vector_to_intVector(counted_string_frequencies, string_frequencies_vector, ra)
    (0 until string_ids_vector.getValueCount).map { idx =>
      new String(
        varCharVector.get(string_ids_vector.get(idx)),
        "UTF-8"
      ) -> string_frequencies_vector.get(idx)
    }.toMap
  }

  def non_null_int_vector_to_intVector(
    input: non_null_int_vector,
    intVector: IntVector,
    rootAllocator: RootAllocator
  ): Unit = {
    val res = rootAllocator.newReservation()
    res.add(input.count.toInt)
    val validityBuffer = res.allocateBuffer()
    validityBuffer.reallocIfNeeded(input.count.toInt)
    (0 until input.count.toInt).foreach(i => BitVectorHelper.setBit(validityBuffer, i))

    import scala.collection.JavaConverters._
    intVector.loadFieldBuffers(
      new ArrowFieldNode(input.count, 0),
      List(
        validityBuffer,
        new ArrowBuf(
          validityBuffer.getReferenceManager,
          null,
          input.count * 4,
          Pointer.nativeValue(input.data)
        )
      ).asJava
    )
  }

}
