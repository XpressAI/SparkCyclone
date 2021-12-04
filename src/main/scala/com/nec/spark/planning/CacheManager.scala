package com.nec.spark.planning

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.nec.spark.planning.CEvaluationPlan.HasFloat8Vector.RichObject
import com.nec.ve.VeColBatch
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.memory.{ArrowBuf, BufferLedger, DefaultAllocationManagerFactory}
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BigIntVector, Float8Vector, IntVector, SmallIntVector, ValueVector, VarCharVector}
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, PlanLater, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector

trait CacheElement {
  def name: String
}
case class FixedWidthCacheElement(size: Int,
                                  nullCount: Int,
                                  dataAddress: Long,
                                  validityAddress: Long,
                                  dataSize: Long,
                                  override val name: String) extends CacheElement
object FixedWidthCacheElement{
}
case class VariableWidthCacheElement(size: Int,
                                     nullCount: Int,
                                     dataAddress: Long,
                                     validityAddress: Long,
                                     offsetsAddress: Long,
                                     dataSize: Long,
                                     offsetSize: Long,
                                     override val name: String) extends CacheElement
object CacheManager {
  var cacheMap: Map[String, List[VeColVector]] = Map.empty

  def isCached(plan: SparkPlan): Boolean = {
    val outputNames = plan.output.map(_.name)

    ((plan.isInstanceOf[LeafExecNode] || plan.isInstanceOf[SerializeFromObjectExec])
      && !plan.isInstanceOf[PlanLater]) && outputNames.forall(cacheMap.contains(_))
  }

  def clear(): Unit = {
    cacheMap = Map.empty
  }

  def cacheTable(batches: List[VeColBatch]): Unit = {
    synchronized {
      val cachedElements = batches.map(_.cols).map(cols => (cols.head.name, cols))
      cacheMap ++= cachedElements
    }
  }

  def getCachedTable(attributes: Seq[Attribute]): List[VeColBatch] = {
    val allColumns = attributes.map(_.name).flatMap(cacheMap(_).zipWithIndex)

    allColumns.groupBy(_._2)
      .map(_._2.map(_._1))
      .map(vectors => VeColBatch.fromList(vectors.toList))
      .toList
  }
}
