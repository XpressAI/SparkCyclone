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
import java.nio.ByteOrder
import java.nio.Buffer
import com.nec.CountStringsLibrary.varchar_vector_raw

object WordCount {
  def wrapAddress(addr: Long, length: Int): ByteBuffer = {
    val address = classOf[Buffer].getDeclaredField("address")
    address.setAccessible(true)
    val capacity = classOf[Buffer].getDeclaredField("capacity")
    capacity.setAccessible(true)

    val bb = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder())
    address.setLong(bb, addr)
    capacity.setInt(bb, length)
    bb.clear()
    bb
  }

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
      /** after allocating, this pointer now contains a value of the VE storage address **/
      veInputPointer.get(),
      new org.bytedeco.javacpp.Pointer(byteBuffer),
      size
    )
    veInputPointer.get()
  }

  def wordCountArrowVE(
    proc: Aurora.veo_proc_handle,
    ctx: Aurora.veo_thr_ctxt,
    lib: Long,
    varCharVector: VarCharVector
  ): Map[String, Long] = {
    val our_args = Aurora.veo_args_alloc()
    try {
      with_veo_varchar_vector(proc, varCharVector) { varchar_vector_raw =>

        val v_bb = varchar_vector_raw.getPointer.getByteBuffer(0, 20)
        v_bb.putLong(0, varchar_vector_raw.data)
        v_bb.putLong(8, varchar_vector_raw.offsets)
        v_bb.putInt(16, varchar_vector_raw.count)
        Aurora.veo_args_set_stack(
          our_args,
          0,
          0,
          v_bb,
          20L
        )

        val counted_string_ids = new non_null_int_vector()
        val counted_bb = counted_string_ids.getPointer.getByteBuffer(0, 12)
        Aurora.veo_args_set_stack(
          our_args,
          1,
          1,
          counted_bb,
          16
        )


        val counted_string_frequencies = new non_null_int_vector()
        val freq_bb = counted_string_frequencies.getPointer.getByteBuffer(0, 12)
        Aurora.veo_args_set_stack(
          our_args,
          1,
          2,
          freq_bb,
          16
        )

        val req_id = Aurora.veo_call_async_by_name(ctx, lib, count_strings, our_args)
        val fnCallResult = new LongPointer(8)
        val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
        require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
        require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")
        veo_read_non_null_int_vector(proc, counted_string_ids, counted_bb)
        veo_read_non_null_int_vector(proc, counted_string_frequencies, freq_bb)
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
    f: varchar_vector_raw => T
  ): T = {
    val vcvr = new varchar_vector_raw()
    vcvr.count = varCharVector.getValueCount
    vcvr.data = copyBufferToVe(proc, varCharVector.getDataBuffer.nioBuffer())
    vcvr.offsets = copyBufferToVe(proc, varCharVector.getOffsetBuffer.nioBuffer())
    f(vcvr)
  }

  /** Take a vec, and rewrite the pointer to our local so we can read it */
  /** Todo consider to deallocate from VE! unless we pass it onward */
  def veo_read_non_null_int_vector(proc: Aurora.veo_proc_handle, vec: non_null_int_vector, byteBuffer: ByteBuffer): Unit = {
    val veoPtr = byteBuffer.getLong(0)
    val dataCount = byteBuffer.getInt(8)
    val dataSize = dataCount * 8
    val vhTarget = ByteBuffer.allocateDirect(dataSize)
    Aurora.veo_read_mem(
      proc,
      new org.bytedeco.javacpp.Pointer(vhTarget),
      veoPtr,
      dataSize
    )
    vec.count = dataCount
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
    val counted_strings = counted_string_ids.count.toInt * 4
    val ra = new RootAllocator()
    val res = ra.newReservation()
    res.reserve(counted_strings)
    val string_ids_vector = new IntVector("id", ra)
    non_null_int_vector_to_intVector(counted_string_ids, string_ids_vector, ra)
    val string_frequencies_vector = new IntVector("frequency", ra)
    non_null_int_vector_to_intVector(counted_string_frequencies, string_frequencies_vector, ra)
    (0 until string_ids_vector.getValueCount).map { idx =>
      val freq = string_frequencies_vector.get(idx)
      val stringId = string_ids_vector.get(idx)
      val strValue = new String(
        varCharVector.get(stringId),
        "UTF-8"
      )
      (strValue, freq)
    }.toMap
  }

  def non_null_int_vector_to_intVector(
    input: non_null_int_vector,
    intVector: IntVector,
    rootAllocator: RootAllocator
  ): Unit = {

    /** Set up the validity buffer -- everything is valid here **/
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
