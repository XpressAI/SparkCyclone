package com.nec.ve

import com.nec.cache.DualMode.unwrapPossiblyDualToVeColBatches
import com.nec.cache.{ArrowEncodingSettings, CycloneCacheBase}
import com.nec.colvector.SparkSqlColumnVectorConversions._
import com.nec.colvector.{VeColBatch, VeColVectorSource}
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.ve.VeProcess.{DeferredVeProcess, OriginalCallingContext, WrappingVeo}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec

import scala.collection.JavaConverters.asScalaIteratorConverter

@org.scalatest.Ignore()
@VectorEngineTest
final class DualModeVESpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  import OriginalCallingContext.Automatic._

  "A dataset from ColumnarBatches can be read via the carrier columnar vector" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>
    implicit val source: VeColVectorSource = VeColVectorSource(
      s"Process ${SparkCycloneExecutorPlugin._veo_proc}, executor ${SparkEnv.get.executorId}"
    )

    implicit val veProc: VeProcess =
      DeferredVeProcess(() =>
        WrappingVeo(SparkCycloneExecutorPlugin._veo_proc, SparkCycloneExecutorPlugin._veo_thr_ctxt, source, VeProcessMetrics.noOp)
      )

    def makeColumnarBatch1() = {
      val vec1 = {
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        new IntVector("test", rootAllocator)
      }
      vec1.setValueCount(5)
      vec1.setSafe(0, 10)
      vec1.setSafe(1, 20)
      vec1.setSafe(2, 30)
      vec1.setSafe(3, 40)
      vec1.setSafe(4, 50)
      new ColumnarBatch(Array(new ArrowColumnVector(vec1)), 5)
    }

    def makeColumnarBatch2() = {
      val vec2 = {
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        new IntVector("test", rootAllocator)
      }
      vec2.setValueCount(4)
      vec2.setSafe(0, 60)
      vec2.setSafe(1, 70)
      vec2.setSafe(2, 80)
      vec2.setSafe(3, 90)
      new ColumnarBatch(Array(new ArrowColumnVector(vec2)), 4)
    }

    val inputRdd: RDD[InternalRow] = sparkSession.sparkContext
      .makeRDD(Seq(1, 2))
      .repartition(1)
      .map { int =>
        import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics._
        VeColBatch.fromArrowColumnarBatch(
          if (int == 1) makeColumnarBatch1()
          else makeColumnarBatch2()
        )
      }
      .mapPartitions(it => it.flatMap(_.toSparkColumnarBatch.rowIterator.asScala))
    val result = inputRdd
      .mapPartitions { iteratorRows =>
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        implicit val arrowEncodingSettings: ArrowEncodingSettings =
          ArrowEncodingSettings("UTC", 3, 10)
        import com.nec.spark.SparkCycloneExecutorPlugin.ImplicitMetrics._
        unwrapPossiblyDualToVeColBatches(
          possiblyDualModeInternalRows = iteratorRows,
          arrowSchema =
            CycloneCacheBase.makeArrowSchema(Seq(AttributeReference("test", IntegerType)()))
        )
      }
      .mapPartitions { veColBatches =>
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        veColBatches
          .map(_.toArrowColumnarBatch)
          .map(cb => cb.column(0).getArrowValueVector)
          .flatMap(fv => (0 until fv.getValueCount).map(idx => fv.asInstanceOf[IntVector].get(idx)))
      }
      .collect()
      .toList
      .sorted

    assert(result == List(10, 20, 30, 40, 50, 60, 70, 80, 90))
  }
}
