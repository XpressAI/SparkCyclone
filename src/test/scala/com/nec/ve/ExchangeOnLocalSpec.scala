package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.spark.SparkAdditions
import com.nec.ve.PureVeFunctions.PartitioningFunction
import com.nec.ve.VERDDSpec.{exchangeBatches, MultiFunctionName}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

final class ExchangeOnLocalSpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  "Exchange data across partitions in local mode (ExchangeLocal)" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
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
