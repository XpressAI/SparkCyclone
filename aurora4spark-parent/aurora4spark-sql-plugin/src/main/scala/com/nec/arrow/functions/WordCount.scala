package com.nec.arrow.functions
import com.nec.arrow.ArrowNativeInterface
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.memory.RootAllocator

object WordCount {

  val WordCountSourceCode: String = {
    val source = scala.io.Source.fromInputStream(getClass.getResourceAsStream("word-count.c"))
    try source.mkString
    finally source.close()
  }

  def runOn(
    nativeInterface: ArrowNativeInterface
  )(varCharVector: VarCharVector): Map[String, Int] = {
    val ra = new RootAllocator()
    val idVector = new IntVector("id", ra)
    val frequencyVector = new IntVector("frequency", ra)

    if (true) {
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
    } else {
      (0 until varCharVector.getValueCount()).view.map { stringId =>
        val strValue = new String(varCharVector.get(stringId), "UTF-8")
        (strValue, 1)
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
