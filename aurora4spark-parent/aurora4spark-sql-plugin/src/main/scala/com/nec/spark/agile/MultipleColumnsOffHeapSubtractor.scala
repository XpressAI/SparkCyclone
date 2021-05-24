package com.nec.spark.agile

import com.nec.spark.Aurora4SparkExecutorPlugin
import com.nec.{SubSimple, VeJavaContext}
import sun.misc.Unsafe


trait MultipleColumnsOffHeapSubtractor extends Serializable {
  def subtract(inputMemoryAddress: Long, count: Int): Double
}

object MultipleColumnsOffHeapSubtractor {

  object UnsafeBased extends MultipleColumnsOffHeapSubtractor {
    private def getUnsafe: Unsafe = {
      val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
      theUnsafe.setAccessible(true)
      theUnsafe.get(null).asInstanceOf[Unsafe]
    }

    def subtract(inputMemoryAddress: Long, count: Int): Double = {
      val result = (0 until count)
        .map(index => getUnsafe.getDouble(inputMemoryAddress + index * 8))
        .reduce((a, b) => a - b)

      result
    }
  }

  case object VeoBased extends MultipleColumnsOffHeapSubtractor {

    override def subtract(inputMemoryAddress: Long, count: Int): Double = {
      val vej =
        new VeJavaContext(Aurora4SparkExecutorPlugin._veo_ctx, Aurora4SparkExecutorPlugin.lib)
      SubSimple.subtract_doubles_mem(vej, inputMemoryAddress, count)
    }
  }
}
