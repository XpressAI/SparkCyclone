package com.nec.ve.exchange

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.SparkAdditions
import com.nec.ve.DetectVectorEngineSpec.VeClusterConfig
import com.nec.ve.PureVeFunctions.PartitioningFunction
import ExchangeBatches._
import com.nec.ve.{DynamicVeSqlExpressionEvaluationSpec, VeKernelInfra}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

final class ExchangeOnClusterSpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  "Exchange data across partitions in cluster mode (ExchangeCluster)" in withSparkSession2(
    VeClusterConfig.andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
  ) { sparkSession =>
    val result =
      compiledWithHeaders(PartitioningFunction, MultiFunctionName) { path =>
        val pathStr = path.toString
        exchangeBatches(sparkSession, pathStr)
          .collect()
          .toList
          .toSet
      }

    val expected = List[Double](199, 299, 399, 500).toSet
    expect(result == expected)
  }

}
