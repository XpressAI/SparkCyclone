package com.nec.ve

import com.nec.cache.DualMode.unwrapPossiblyDualToVeColBatches
import com.nec.cache.{ArrowEncodingSettings, CycloneCacheBase}
import com.nec.colvector.SparkSqlColumnVectorConversions._
import com.nec.colvector.{VeColBatch, VeColVectorSource}
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.compiler.VeKernelInfra
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.util.CallContextOps._
import com.nec.vectorengine._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.freespec.AnyFreeSpec
import scala.collection.JavaConverters.asScalaIteratorConverter

@Ignore
@VectorEngineTest
final class DualModeVESpec
  extends AnyFreeSpec
  with SparkAdditions
  with VeKernelInfra
  with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    super.beforeAll
    SparkCycloneExecutorPlugin.veProcess = VeProcess.create(-1, getClass.getName, 2)
  }

  override def afterAll(): Unit = {
    SparkCycloneExecutorPlugin.veProcess.freeAll
    SparkCycloneExecutorPlugin.veProcess.close
    super.afterAll
  }

  "A dataset from ColumnarBatches can be read via the carrier columnar vector" in withSparkSession2(
    DynamicVeSqlExpressionEvaluationSpec.VeConfiguration
  ) { sparkSession =>

    implicit val source = SparkCycloneExecutorPlugin.source
    implicit val veProcess: VeProcess = SparkCycloneExecutorPlugin.veProcess

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
        import SparkCycloneExecutorPlugin.veMetrics
        implicit val source = SparkCycloneExecutorPlugin.source
        VeColBatch.fromArrowColumnarBatch(
          if (int == 1) makeColumnarBatch1()
          else makeColumnarBatch2()
        )
      }
      .mapPartitions(it => it.flatMap(_.toSparkColumnarBatch.rowIterator.asScala))
    val result = inputRdd
      .mapPartitions { iteratorRows =>
        implicit val rootAllocator: RootAllocator = new RootAllocator()
        implicit val encoding: ArrowEncodingSettings =
          ArrowEncodingSettings("UTC", 3, 10)
        import com.nec.spark.SparkCycloneExecutorPlugin.veMetrics
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
