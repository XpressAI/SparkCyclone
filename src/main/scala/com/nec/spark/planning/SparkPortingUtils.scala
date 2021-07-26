package com.nec.spark.planning

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

object SparkPortingUtils {
  class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    def tryOrIOException[T](block: => T): T = {
      try {
        block
      } catch {
        case e: IOException =>
          throw e
        case NonFatal(e) =>
          throw new IOException(e)
      }
    }

      def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
      out.defaultWriteObject()
      value.write(out)
    }

      def readObject(in: ObjectInputStream): Unit = tryOrIOException {
      value = new Configuration(false)
      value.readFields(in)
    }
  }
  case class ResourceId(resourceName: String)
  case class ResourceRequest(id: ResourceId, amount: Int)
  object ScanOperation {
    def unapply(logicalPlan: LogicalPlan): Option[(Seq[NamedExpression], Expression, LogicalRelation)] = {
      throw new RuntimeException("Just for porting")
    }
  }
  case class RowToColumnarExec(sparkPlan: SparkPlan) extends SparkPlan{
    override protected def doExecute(): RDD[InternalRow] = {
      throw new RuntimeException("Just for porting")
    }

    override def output: Seq[Attribute] = {
      throw new RuntimeException("Just for porting")
    }

    override def children: Seq[SparkPlan] = {
      throw new RuntimeException("Just for porting")
    }
  }
  implicit class PortedSparkPlan(sparkPlan: SparkPlan) {
    def supportsColumnar: Boolean = false
    def executeColumnar(): RDD[ColumnarBatch] ={
      throw new RuntimeException("Not implemented")
    }
  }

  case class ResourceInformation(name: String, resources: Array[String])


}
