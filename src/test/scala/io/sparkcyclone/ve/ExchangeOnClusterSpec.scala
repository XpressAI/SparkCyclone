package io.sparkcyclone.ve

import com.eed3si9n.expecty.Expecty.expect
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.native.compiler.VeKernelInfra
import io.sparkcyclone.spark.SparkAdditions
import io.sparkcyclone.ve.DetectVectorEngineSpec.VeClusterConfig
import io.sparkcyclone.vectorengine.SampleVeFunctions.PartitioningFunction
import io.sparkcyclone.ve.VERDDSpec.exchangeBatches
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
      withCompiled(PartitioningFunction) { path =>
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
