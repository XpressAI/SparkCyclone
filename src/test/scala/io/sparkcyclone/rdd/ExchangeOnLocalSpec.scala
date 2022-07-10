package io.sparkcyclone.rdd

import com.eed3si9n.expecty.Expecty.expect
import io.sparkcyclone.annotations.VectorEngineTest
import io.sparkcyclone.native.compiler.VeKernelInfra
import io.sparkcyclone.spark.SparkAdditions
import io.sparkcyclone.vectorengine._
import io.sparkcyclone.vectorengine.SampleVeFunctions.PartitioningFunction
import io.sparkcyclone.plugin.SparkCycloneExecutorPlugin
import io.sparkcyclone.rdd.VERDDSpec.exchangeBatches
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

@VectorEngineTest
final class ExchangeOnLocalSpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    SparkCycloneExecutorPlugin.veProcess = DeferredVeProcess { () =>
      // Keep the number of VE cores to a minimum during test
      VeProcess.create(-1, getClass.getName, 2)
    }
  }

  "Exchange data across partitions in local mode (ExchangeLocal)" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
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
