package io.sparkcyclone.rdd

import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.data.conversion.ArrayTConversions._
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.native.compiler.VeKernelInfra
import io.sparkcyclone.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import io.sparkcyclone.rdd.DetectVectorEngineSpec.VeClusterConfig
import io.sparkcyclone.data.vector.{VeColBatch, VeColVector}
import io.sparkcyclone.util.CallContext
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

@VectorEngineTest
final class JoinRDDSpec extends AnyFreeSpec with SparkAdditions with VeKernelInfra {
  def testJoin(sparkSession: SparkSession): Seq[(Seq[Double], Seq[Double])] = {
    val partsL: Seq[(Int, Seq[Double])] =
      Seq(1 -> Seq(3, 4, 5), 2 -> Seq(5, 6, 7))
    val partsR: Seq[(Int, Seq[Double])] =
      Seq(1 -> Seq(5, 6, 7), 2 -> Seq(8, 8, 7), 3 -> Seq(9, 6, 7))

    import SparkCycloneExecutorPlugin._

    VeRDDOps
      .joinExchange(
        sparkSession.sparkContext.makeRDD(partsL).map { case (i, l) =>
          import io.sparkcyclone.util.CallContextOps._
          i -> VeColBatch(List(l.toArray.toBytePointerColVector("left").toVeColVector))
        },
        sparkSession.sparkContext.makeRDD(partsR).map { case (i, l) =>
          import io.sparkcyclone.util.CallContextOps._
          i -> VeColBatch(List(l.toArray.toBytePointerColVector("right").toVeColVector))
        },
        cleanUpInput = true
      )
      .map { case (la, lb) =>
        ???
        // TODO: fix up test cases
        //(la.columns.flatMap(_.toSeq), lb.columns.flatMap(_.toSeq))
      }
      .collect()
      .toSeq
  }


  "Join data across partitioned data (Local mode)" ignore {
    val result =
      withSparkSession2(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration) { sparkSession =>
        testJoin(sparkSession)
      }.sortBy(_._1.head)

    val expected: Seq[(Seq[Double], Seq[Double])] =
      Seq(
        Seq[Double](3, 4, 5) -> Seq[Double](5, 6, 7),
        Seq[Double](5, 6, 7) -> Seq[Double](8, 8, 7)
      )

    assert(result == expected)
  }

  "Join data across partitioned data (Cluster mode)" ignore {
    val result =
      withSparkSession2(
        VeClusterConfig
          .andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
      ) { sparkSession =>
        testJoin(sparkSession)
      }.sortBy(_._1.head)

    val expected: Seq[(Seq[Double], Seq[Double])] =
      Seq(
        Seq[Double](3, 4, 5) -> Seq[Double](5, 6, 7),
        Seq[Double](5, 6, 7) -> Seq[Double](8, 8, 7)
      )

    assert(result == expected)
  }
}
