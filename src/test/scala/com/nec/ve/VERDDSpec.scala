package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.colvector.BytePointerColVector
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core.VeNullableDouble
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics._
import com.nec.ve.PureVeFunctions.DoublingFunction
import com.nec.ve.VeProcess.OriginalCallingContext
import com.nec.ve.VeRDDOps.RichKeyedRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

@VectorEngineTest
final class VERDDSpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  import OriginalCallingContext.Automatic._

  "We can perform a VE call on Arrow things" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>
    import SparkCycloneExecutorPlugin._

    val result = compiledWithHeaders(DoublingFunction, "f") { path =>
      val ref = veProcess.loadLibrary(path)

      VERDDSpec.doubleBatches {
        sparkSession.sparkContext
          .range(start = 1, end = 500, step = 1, numSlices = 4)
          .map(_.toDouble)
      }
      .map { input =>
        val colvec = input.toVeColVector
        val outputs = veProcess.execute(ref, "f", List(colvec), DoublingFunction.outputs)
        outputs.head.toBytePointerVector.toSeqOpt[Double].flatten
      }
      .collect
      .toSeq
      .flatten
      .sorted
    }

    val expected = List.range(1, 500).map(_.toDouble).map(_ * 2)
    expect(result == expected)
  }
}

object VERDDSpec {
  val MultiFunctionName = "f_multi"
  import OriginalCallingContext.Automatic._

  def exchangeBatches(sparkSession: SparkSession, pathStr: String): RDD[Double] = {
    import SparkCycloneExecutorPlugin._

    doubleBatches {
      sparkSession.sparkContext
        .range(start = 1, end = 501, step = 1, numSlices = 4)
        .map(_.toDouble)
    }

    .mapPartitions(
      f = { iter =>
        iter.flatMap { input =>
          val colvec = input.toVeColVector
          val ref = veProcess.loadLibrary(java.nio.file.Paths.get(pathStr))

          veProcess.executeMulti(
            ref,
            MultiFunctionName,
            List(colvec),
            List(VeNullableDouble.makeCVector("o_dbl"))
          )
          .map { case (k, vs) => (k, vs.head) }
        }
      },
      preservesPartitioning = true
    )
    .filter(_._2.nonEmpty)
    .exchangeBetweenVEs(cleanUpInput = true)
    .mapPartitions { iter =>
      Iterator.continually {
        iter.flatMap { colvec =>
          val output = colvec.toBytePointerVector.toSeqOpt[Double].flatten
          if (output.isEmpty) None else Some(output.max)
        }.max
      }
      .take(1)
    }
  }

  def doubleBatches(rdd: RDD[Double]): RDD[BytePointerColVector] = {
    implicit val source = VeColBatch.VeColVectorSource("source")
    rdd.mapPartitions { iter =>
      Iterator.continually {
        iter.toSeq.map(Some(_)).toBytePointerColVector("input")
      }
      .take(1)
    }
  }
}
