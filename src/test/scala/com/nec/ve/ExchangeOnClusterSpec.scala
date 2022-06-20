package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.compiler.VeKernelInfra
import com.nec.spark.SparkAdditions
import com.nec.ve.DetectVectorEngineSpec.VeClusterConfig
import com.nec.vectorengine.SampleVeFunctions.PartitioningFunction
import com.nec.ve.VERDDSpec.exchangeBatches
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

@VectorEngineTest
final class ExchangeOnClusterSpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  "Exchange data across partitions in cluster mode (ExchangeCluster)" ignore withSparkSession2(
    VeClusterConfig.andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
  ) { sparkSession =>
    val result =
      compiledWithHeaders(PartitioningFunction) { path =>
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
