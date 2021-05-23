package com.nec

import com.nec.CountStringsLibrary.non_null_int_vector
import com.nec.aurora.Aurora
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector}

import java.nio.file.Path
import scala.language.higherKinds
import com.nec.native.ArrowInterfaces.non_null_int_vector_to_intVector
import com.nec.native.{CNativeInterfaces, VeNativeInterfaces}

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

  def wordCountArrowVE[T](
    proc: Aurora.veo_proc_handle,
    ctx: Aurora.veo_thr_ctxt,
    lib: Long,
    varCharVector: VarCharVector
  )(f: WordCountResults => T): T = {

    val ra = new RootAllocator()
    val idVector = new IntVector("id", ra)
    val frequencyVector = new IntVector("frequency", ra)
    val inputArguments: List[Option[VarCharVector]] = List(Some(varCharVector), None, None)
    val outputArguments: List[Option[IntVector]] =
      List(None, Some(idVector), Some(frequencyVector))

    VeNativeInterfaces.executeVe(
      proc = proc,
      ctx = ctx,
      lib = lib,
      functionName = CountStringsFunctionName,
      inputArguments = inputArguments,
      outputArguments = outputArguments
    )

    f(
      new WordCountResults(
        string_ids_vector = idVector,
        string_frequencies_vector = frequencyVector
      )
    )
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
    val ra = new RootAllocator()
    val idVector = new IntVector("id", ra)
    val frequencyVector = new IntVector("frequency", ra)
    val inputArguments: List[Option[VarCharVector]] = List(Some(varCharVector), None, None)
    val outputArguments: List[Option[IntVector]] =
      List(None, Some(idVector), Some(frequencyVector))

    CNativeInterfaces.executeC(libPath, CountStringsFunctionName, inputArguments, outputArguments)

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
