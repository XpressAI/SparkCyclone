package com.nec.ve

import com.nec.spark.SparkCycloneExecutorPlugin.veProcess
import com.nec.spark.agile.CFunctionGeneration.CVector
import com.nec.spark.agile.core.CFunction2
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector

import java.nio.file.Paths

class MappedVeRDD(rdd: VeRDD[Int], func: CFunction2, soPath: String, outputs: List[CVector]) extends VeRDD[Int](rdd) {
  override def reduce(f: (Int, Int) => Int): Int = {

    val start1 = System.nanoTime()

    val results = rdd.inputs.mapPartitions { inputIterator =>
      //val start3 = System.nanoTime()
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val libRef = veProcess.loadLibrary(Paths.get(soPath))
      val batches = inputIterator.toList
      val iter = batches.map { batch =>
        evalFunction(func, libRef, batch.cols, outputs)
      }
      //val end3 = System.nanoTime()
      //println(s"reducing... took ${(end3 - start3) / 1000000000.0}s ")
      iter.toIterator
    }

    val end1 = System.nanoTime()

    println(s"evalFunction took ${(end1 - start1) / 1000000000.0}s")

    val start2 = System.nanoTime()

    val out2 = results.mapPartitions { veColBatch =>
      val start4 = System.nanoTime()

      implicit val allocator: RootAllocator = new RootAllocator(Int.MaxValue)

      val batches = veColBatch.toList
      val r = batches.flatMap(_.cols).flatMap { veColVector =>
        val intVec = veColVector.toArrowVector().asInstanceOf[IntVector]
        val ids = (0 until intVec.getValueCount)
        ids.map(intVec.get).toList
      }
      val end4 = System.nanoTime()
      println(s"resultsing took ${(end4 - start4) / 1000000000.0}")
      r.toIterator
    }.collect().reduce(f)

    val end2 = System.nanoTime()

    println(s"results.map took ${(end2 - start2) / 1000000000.0}s")

    out2
  }
}
