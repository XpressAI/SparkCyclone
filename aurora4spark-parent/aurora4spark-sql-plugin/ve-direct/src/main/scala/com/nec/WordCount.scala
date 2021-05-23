package com.nec

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VarCharVector}

import scala.language.higherKinds
import com.nec.native.ArrowNativeInterface

trait WordCount {
  def countWords(varCharVector: VarCharVector): (IntVector, IntVector)
}

object WordCount {

  val WordCountSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/sort-stuff-lib.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterface
  )(varCharVector: VarCharVector): Map[String, Int] = {
    val ra = new RootAllocator()
    val idVector = new IntVector("id", ra)
    val frequencyVector = new IntVector("frequency", ra)

    nativeInterface.callFunction(
      name = "count_strings",
      inputArguments = List(Some(varCharVector), None, None),
      outputArguments = List(None, Some(idVector), Some(frequencyVector))
    )

    new WordCountResults(string_ids_vector = idVector, string_frequencies_vector = frequencyVector)
      .toMap(varCharVector)
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

  def wordCountJVM(varCharVector: VarCharVector): Map[String, Int] = {
    (0 until varCharVector.getValueCount)
      .map(idx => new String(varCharVector.get(idx), "utf-8"))
      .groupBy(identity)
      .mapValues(_.length)
  }

}
