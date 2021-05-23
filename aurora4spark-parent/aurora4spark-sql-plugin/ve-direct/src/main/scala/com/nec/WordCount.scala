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
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/word-count.c"))
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

    (0 until idVector.getValueCount).map { idx =>
      val freq = frequencyVector.get(idx)
      val stringId = idVector.get(idx)
      val strValue = new String(varCharVector.get(stringId), "UTF-8")
      (strValue, freq)
    }.toMap
  }

  def wordCountJVM(varCharVector: VarCharVector): Map[String, Int] = {
    (0 until varCharVector.getValueCount)
      .map(idx => new String(varCharVector.get(idx), "utf-8"))
      .groupBy(identity)
      .mapValues(_.length)
  }

}
