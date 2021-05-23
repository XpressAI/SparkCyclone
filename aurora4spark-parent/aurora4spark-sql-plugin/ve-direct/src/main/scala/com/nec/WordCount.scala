package com.nec

import com.nec.CountStringsLibrary.{non_null_int_vector, varchar_vector_raw}
import com.nec.WordCount.ArgumentType.{InputVarCharVector, OutputIntVector}
import com.nec.aurora.Aurora
import com.sun.jna.Library
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector}
import org.bytedeco.javacpp.LongPointer

import java.nio.file.Path
import scala.language.higherKinds
import com.nec.native.ArrowInterfaces.{c_varchar_vector, non_null_int_vector_to_intVector}
import com.nec.native.VeNativeInterfaces.{make_veo_varchar_vector, veo_read_non_null_int_vector}

import java.nio.ByteBuffer

trait WordCount {
  def countWords(varCharVector: VarCharVector): (IntVector, IntVector)
}

object WordCount {

  val CountStringsFunctionName = "count_strings"

  val WordCountSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/sort-stuff-lib.c"))
    try source.mkString
    finally source.close()
  }

  /** Workaround - not sure why it does not work immediately */
  private def varcharVectorRawToByteBuffer(varchar_vector_raw: varchar_vector_raw): ByteBuffer = {
    val v_bb = varchar_vector_raw.getPointer.getByteBuffer(0, 20)
    v_bb.putLong(0, varchar_vector_raw.data)
    v_bb.putLong(8, varchar_vector_raw.offsets)
    v_bb.putInt(16, varchar_vector_raw.count)
    v_bb
  }

  final class WordCountResults(string_ids_vector: IntVector, string_frequencies_vector: IntVector) {
    def toMap(varCharVector: VarCharVector): Map[String, Int] = {
      (0 until string_ids_vector.getValueCount).map { idx =>
        val freq = string_frequencies_vector.get(idx)
        val stringId = string_ids_vector.get(idx)
        val strValue = new String(varCharVector.get(stringId), "UTF-8")
        (strValue, freq)
      }.toMap
    }
  }

  sealed trait ArgumentType {}
  object ArgumentType {
    case object InputVarCharVector extends ArgumentType
    case object OutputIntVector extends ArgumentType
  }

  type ArgumentsSpec = List[ArgumentType]

  def wordCountArrowVE[T](
    proc: Aurora.veo_proc_handle,
    ctx: Aurora.veo_thr_ctxt,
    lib: Long,
    varCharVector: VarCharVector
  )(f: WordCountResults => T): T = {
    val our_args = Aurora.veo_args_alloc()
    try {

      val ra = new RootAllocator()
      val idVector = new IntVector("id", ra)
      val frequencyVector = new IntVector("frequency", ra)
      val argumentsSpec: ArgumentsSpec = List(InputVarCharVector, OutputIntVector, OutputIntVector)
      val inputArguments: List[Option[VarCharVector]] = List(Some(varCharVector), None, None)
      val outputArguments: List[Option[IntVector]] =
        List(None, Some(idVector), Some(frequencyVector))

      inputArguments.zipWithIndex
        .collect { case (Some(inputVarChar), idx) =>
          inputVarChar -> idx
        }
        .foreach { case (inputVarChar, index) =>
          val varchar_vector_raw = make_veo_varchar_vector(proc, inputVarChar)
          Aurora.veo_args_set_stack(
            our_args,
            0,
            index,
            varcharVectorRawToByteBuffer(varchar_vector_raw),
            20L
          )
        }

      val outputArgumentsVectors: List[(IntVector, Int)] = outputArguments.zipWithIndex.collect {
        case (Some(intVector), index) => intVector -> index
      }

      val outputArgumentsStructs: List[(non_null_int_vector, Int)] = outputArgumentsVectors.map {
        case (intVector, index) =>
          new non_null_int_vector() -> index
      }

      val outputArgumentsByteBuffers: List[(ByteBuffer, Int)] = outputArgumentsStructs.map {
        case (struct, index) =>
          struct.getPointer.getByteBuffer(0, 12) -> index
      }

      outputArgumentsByteBuffers.foreach { case (byteBuffer, index) =>
        Aurora.veo_args_set_stack(our_args, 1, index, byteBuffer, byteBuffer.limit())
      }

      val req_id = Aurora.veo_call_async_by_name(ctx, lib, CountStringsFunctionName, our_args)
      val fnCallResult = new LongPointer(8)
      val callRes = Aurora.veo_call_wait_result(ctx, req_id, fnCallResult)
      require(callRes == 0, s"Expected 0, got $callRes; means VE call failed")
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      (outputArgumentsVectors.zip(outputArgumentsStructs).zip(outputArgumentsByteBuffers)).foreach {
        case (((intVector, _), (non_null_int_vector, _)), (byteBuffer, _)) =>
          veo_read_non_null_int_vector(proc, non_null_int_vector, byteBuffer)
          non_null_int_vector_to_IntVector(non_null_int_vector, intVector)
      }
      f(
        new WordCountResults(
          string_ids_vector = idVector,
          string_frequencies_vector = frequencyVector
        )
      )
    } finally Aurora.veo_args_free(our_args)
  }

  def wordCountJVM(varCharVector: VarCharVector): Map[String, Int] = {
    (0 until varCharVector.getValueCount)
      .map(idx => new String(varCharVector.get(idx), "utf-8"))
      .groupBy(identity)
      .mapValues(_.length)
  }

  def wordCountArrowCC[T](libPath: Path, varCharVector: VarCharVector)(
    f: WordCountResults => T
  ): T = {
    // will abstract this out later
    import scala.collection.JavaConverters._
    val nativeLibraryHandler =
      new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
    val nl = nativeLibraryHandler.getNativeLibrary
    val fn = nl.getFunction(CountStringsFunctionName)

    val ra = new RootAllocator()
    val idVector = new IntVector("id", ra)
    val frequencyVector = new IntVector("frequency", ra)
    val argumentsSpec: ArgumentsSpec = List(InputVarCharVector, OutputIntVector, OutputIntVector)
    val inputArguments: List[Option[VarCharVector]] = List(Some(varCharVector), None, None)
    val outputArguments: List[Option[IntVector]] =
      List(None, Some(idVector), Some(frequencyVector))

    val outputStructs = outputArguments.map(_.map(intVector => new non_null_int_vector()))

    val invokeArgs: Array[java.lang.Object] = inputArguments
      .zip(outputStructs)
      .map {
        case ((Some(vcv), _)) =>
          c_varchar_vector(vcv)
        case ((_, Some(structIntVector))) =>
          structIntVector
        case other =>
          sys.error(s"Unexpected state: $other")
      }
      .toArray

    fn.invokeLong(invokeArgs)

    outputStructs.zip(outputArguments).foreach {
      case (Some(struct), Some(vec)) =>
        non_null_int_vector_to_IntVector(struct, vec)
      case _ =>
    }
    f(
      new WordCountResults(
        string_ids_vector = idVector,
        string_frequencies_vector = frequencyVector
      )
    )
  }

  def non_null_int_vector_to_IntVector(input: non_null_int_vector, output: IntVector): Unit = {
    val nBytes = input.count * 4
    output.getAllocator.newReservation().reserve(nBytes)
    non_null_int_vector_to_intVector(input, output, output.getAllocator)
  }

}
