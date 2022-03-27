package com.nec.ve

import com.nec.arrow.colvector.ArrayTConversions.ArrayTToBPCV
import com.nec.spark.SparkCycloneDriverPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics.processMetrics
import com.nec.spark.agile.core.CFunction2.CFunctionArgument.PointerPointer
import com.nec.spark.agile.core.CFunction2.DefaultHeaders
import com.nec.spark.agile.core.{CFunction2, CVector, VeNullableLong}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.nio.file.Paths
import scala.language.experimental.macros
import scala.language.implicitConversions

object SequenceVeRDD {
  def makeSequence(sc: SparkContext, start: Long, endInclusive: Long): SequenceVeRDD = {
    val resources = sc.resources.get("nec.com/ve").map(_.addresses.length).getOrElse(1)
    val cores = resources * 8L
    val rdd = sc.parallelize(0L until cores, cores.toInt)

    val code = s"""
    |  int64_t start = $start;
    |  int64_t end = $endInclusive;
    |  int64_t total = (end - start) + 1;
    |  int64_t partition = a_in[0]->data[0];
    |  int64_t partitions = $cores;
    |  int64_t per_partition = ceil(total / partitions);
    |  int64_t code_start = start + (partition * per_partition);
    |  int64_t code_end = code_start + per_partition;
    |  out[0] = nullable_bigint_vector::allocate();
    |  out[0]->resize(per_partition);
    |  for (int64_t i = 0; i < per_partition; i++) {
    |    out[0]->data[i] = code_start + i;
    |    //std::cout << out[0]->data[i] << std::endl;
    |  }
    |  for (int64_t i = 0; i < per_partition; i++) {
    |    out[0]->set_validity(i, 1);
    |  }
    |""".stripMargin

    val funcName = s"sequence_${Math.abs(code.hashCode())}"

    val outputs = List(CVector("out", VeNullableLong))
    val func = new CFunction2(
      funcName,
      Seq(
        PointerPointer(CVector("a_in", VeNullableLong)),
        PointerPointer(outputs.head)
      ),
      code,
      DefaultHeaders
    )

    println(s"Generated code:\n${func.toCodeLinesWithHeaders.cCode}")

    // compile
    val compiledPath = SparkCycloneDriverPlugin.currentCompiler.forCode(func.toCodeLinesWithHeaders).toString
    println("compiled path:" + compiledPath)

    new SequenceVeRDD(rdd, rdd.mapPartitions { iter =>
      import com.nec.spark.SparkCycloneExecutorPlugin.{source, veProcess}
      import com.nec.ve.VeProcess.OriginalCallingContext.Automatic.originalCallingContext

      val part = Array[Long](iter.next())
      val colVector = part.toBytePointerColVector(s"seq-${part(0)}")
      val veColVec = colVector.toVeColVector()
      val batch = VeColBatch.fromList(List(veColVec))
      val outVectors = List(CVector("out", VeNullableLong))


      val libRef = veProcess.loadLibrary(Paths.get(compiledPath))

      //val batch = SparkCycloneExecutorPlugin.getCachedBatch("inputs")
      Iterator(VeColBatch.fromList(veProcess.execute(libRef, func.name, batch.cols, outVectors)))
    })
  }
}

class SequenceVeRDD(orig: RDD[Long], rdd: RDD[VeColBatch]) extends BasicVeRDD[Long](orig) {
  override val inputs: RDD[VeColBatch] = rdd.mapPartitionsWithIndex { case (index, valsIter) =>
    println(s"Reading seq for ${index}")
    val batch = valsIter.next()
    Iterator(batch)
  }.persist(StorageLevel.MEMORY_ONLY).cache()
  sparkContext.runJob(inputs, (i: Iterator[_]) => ())
}
