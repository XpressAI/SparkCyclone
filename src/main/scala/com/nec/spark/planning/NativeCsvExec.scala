package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.arrow.vector.Float8Vector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.execution.LeafExecNode
import com.nec.arrow.functions.CsvParse
import com.nec.native.NativeEvaluator
import com.nec.spark.planning.NativeCsvExec.SkipStringsKey
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.WholeTextFileRawRDD.RichSparkContext
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.nio.ByteBuffer

object NativeCsvExec {
  case class NativeCsvStrategy(nativeEvaluator: NativeEvaluator) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      ScanOperation
        .unapply(plan)
        .collect {
          case (a, b, LogicalRelation(rel: HadoopFsRelation, out, cat, iss))
              if rel.fileFormat.isInstanceOf[CSVFileFormat] =>
            NativeCsvExec(
              hadoopRelation = rel,
              output = a.map(_.toAttribute),
              nativeEvaluator = nativeEvaluator
            )
        }
        .orElse {
          PartialFunction.condOpt(plan) {
            case LogicalRelation(rel: HadoopFsRelation, out, cat, iss)
                if rel.fileFormat.isInstanceOf[CSVFileFormat] =>
              NativeCsvExec(hadoopRelation = rel, output = out, nativeEvaluator = nativeEvaluator)
          }
        }
        .toList
    }
  }

  val SkipStringsKey = "spark.com.nec.native-csv-skip-strings"
}

case class NativeCsvExec(
  @transient hadoopRelation: HadoopFsRelation,
  output: Seq[Attribute],
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with LeafExecNode
  with LazyLogging {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = throw new NotImplementedError(
    "Source here is only columnar"
  )
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (sparkContext.getConf.getBoolean(key = SkipStringsKey, defaultValue = true))
      doExecuteColumnarByteArray()
    else
      doExecuteColumnarString()
  }

  protected def doExecuteColumnarByteArray(): RDD[ColumnarBatch] = {
    val numColumns = output.size
    val evaluator = nativeEvaluator.forCode(CsvParse.CsvParseCode)
    val imfi = hadoopRelation.location.asInstanceOf[InMemoryFileIndex]
    sparkContext.wholeRawTextFiles(imfi.rootPaths.head.toString).map { case (name, text) =>
      val allocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"CSV read allocator", 0, Long.MaxValue)

      val outColumns = (0 until numColumns).map { idx =>
        new Float8Vector(s"out_${idx}", allocator)
      }.toList
      val startTime = System.currentTimeMillis()

      evaluator.callFunction(
        name = if (numColumns == 3) "parse_csv" else s"parse_csv_${numColumns}",
        inputArguments = List(
          Some(ByteBufferWrapper(ByteBuffer.wrap(text.getBytes), text.getBytes.length))
        ) ++ outColumns.map(_ => None),
        outputArguments = List(None) ++ outColumns.map(col => Some(Float8VectorWrapper(col)))
      )
      val millis = System.currentTimeMillis() - startTime
      logInfo(s"Took ${millis} ms to process CSV: ${name} (${text.getLength} bytes)")
      new ColumnarBatch(
        outColumns.map(col => new ArrowColumnVector(col)).toArray,
        outColumns.head.getValueCount
      )
    }
  }

  protected def doExecuteColumnarString(): RDD[ColumnarBatch] = {
    val numColumns = output.size
    val evaluator = nativeEvaluator.forCode(CsvParse.CsvParseCode)
    val imfi = hadoopRelation.location.asInstanceOf[InMemoryFileIndex]
    sparkContext.wholeTextFiles(imfi.rootPaths.head.toString).map { case (name, text) =>
      val allocator = ArrowUtilsExposed.rootAllocator
        .newChildAllocator(s"CSV read allocator", 0, Long.MaxValue)

      val outColumns = (0 until numColumns).map { idx =>
        new Float8Vector(s"out_${idx}", allocator)
      }.toList
      val startTime = System.currentTimeMillis()
      evaluator.callFunction(
        name = if (numColumns == 3) "parse_csv" else s"parse_csv_${numColumns}",
        inputArguments =
          List(Some(StringWrapper(new String(text.getBytes)))) ++ outColumns.map(_ => None),
        outputArguments = List(None) ++ outColumns.map(col => Some(Float8VectorWrapper(col)))
      )
      val millis = System.currentTimeMillis() - startTime
      logInfo(s"Took ${millis} ms to process CSV: ${name} (${text.length} bytes)")
      new ColumnarBatch(
        outColumns.map(col => new ArrowColumnVector(col)).toArray,
        outColumns.head.getValueCount
      )
    }
  }

}
