package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CFunctionGeneration
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.tpc.TPCHVESqlSpec
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.CachingSpec.SampleStructure
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.ve.RDDSpec.{doubleBatches, longBatches}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.{DeferredVeProcess, WrappingVeo}
import com.nec.ve.VeRDD.RichKeyedRDD
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

object CachingSpec {
  final case class SampleStructure(str: String, num: Double)
  val SampleItems = List(
    SampleStructure(str = "x", num = 5),
    SampleStructure(str = "yz", num = 6),
    SampleStructure(str = "ab", num = 5),
    SampleStructure(str = "cd", num = 5)
  )
}
final class CachingSpec extends AnyFreeSpec with SparkAdditions with VeKernelInfra {
  "We can retrieve cached items back out" in withSparkSession2(TPCHVESqlSpec.VeConfiguration) {
    sparkSession =>
      import sparkSession.implicits._
      sparkSession
        .createDataset(CachingSpec.SampleItems)
        .repartition(2)
        .cache()
        .createTempView("sample")

      val query = sparkSession.sql("select * from sample where num = 5").as[SampleStructure]
      val plan = query.queryExecution.executedPlan.toString()
      assert(plan.contains("cacheee"), "cache should be there")
      assert(plan.contains("veblahblah"), "ve stuff should be there too")
      val result = query.collect().toList
      assert(result == CachingSpec.SampleItems, "results should be the same")
  }

}
