package com.nec.spark.planning
import java.io.DataInputStream
import java.io.InputStream
import java.nio.ByteBuffer
import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._
import com.nec.arrow.functions.CsvParse
import com.nec.native.NativeEvaluator
import com.nec.spark.planning.NativeCsvExec.SkipStringsKey
import com.nec.spark.planning.NativeCsvExec.transformRawTextFile
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import org.apache.arrow.vector.Float8Vector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.WholeTextFileRawRDD.RichSparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Divide
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

object NativeCsvExec {
  case class NativeCsvStrategy(nativeEvaluator: NativeEvaluator) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      val d = plan match {
        case Project(a, LogicalRelation(rel: HadoopFsRelation, out, cat, iss))
            if rel.fileFormat.isInstanceOf[CSVFileFormat] && isNotAggProjection(plan) =>
          Option(
            NativeCsvExec(
              hadoopRelation = rel,
              output = a.map(_.toAttribute),
              nativeEvaluator = nativeEvaluator
            )
          )

        case lr @ LogicalRelation(rel: HadoopFsRelation, out, cat, iss)
            if rel.fileFormat.isInstanceOf[CSVFileFormat] =>
          Option(
            NativeCsvExec(hadoopRelation = rel, output = out, nativeEvaluator = nativeEvaluator)
          )

        case _ => None

      }
      d.toSeq
    }
  }

  private def isNotAggProjection(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case Project(projectList, child) => {
        projectList.collect {
          case Alias(Add(_, _), name)      =>
          case Alias(Subtract(_, _), name) =>
          case Alias(Multiply(_, _), name) =>
          case Alias(Divide(_, _), name)   =>
        }.size == 0
      }
      case _ => false
    }

  }
  val SkipStringsKey = "spark.com.nec.native-csv-skip-strings"
  val UseIpc = "spark.com.nec.native-csv-ipc"

  def transformRawTextFile(
    numColumns: Int,
    outCols: Int,
    evaluator: ArrowNativeInterfaceNumeric,
    name: String,
    text: Text
  )(implicit logger: Logger): ColumnarBatch = {
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
    val outRows = outColumns.head.getValueCount
    val millis = System.currentTimeMillis() - startTime
    logger.info(s"Took ${millis} ms to process CSV: ${name} (${text.getLength} bytes)")
    val batch = new ColumnarBatch(outColumns.map(col => new ArrowColumnVector(col)).toArray)
    batch.setNumRows(outRows)
    batch
  }

  val bufSize = 16 * 1024

  def maybeDecodePds(
    name: String,
    hadoopConfiguration: SerializableConfiguration,
    portableDataStream: PortableDataStream
  ): InputStream = {
    val original = portableDataStream.open()
    val theCodec =
      new CompressionCodecFactory(hadoopConfiguration.conf).getCodec(new Path(name))

    if (theCodec != null) new DataInputStream(theCodec.createInputStream(original))
    else original
  }

}

case class NativeCsvExec(
  @transient hadoopRelation: HadoopFsRelation,
  output: Seq[Attribute],
  nativeEvaluator: NativeEvaluator
) extends SparkPlan
  with LeafExecNode
  with LazyLogging {

  override protected def doExecute(): RDD[InternalRow] =
    doExecuteColumnar()
      .mapPartitions(batches => {

        val elem = batches.toList.flatMap { batch =>
          (0 until batch.numRows()).map(idx => {
            val row = batch.getRow(idx)
            val newRow = new UnsafeRow(row.numFields)
            val holder = new BufferHolder(newRow)
            val writer = new UnsafeRowWriter(holder, row.numFields)
            holder.reset()
            (0 until row.numFields)
              .foreach(id => writer.write(id, row.getDouble(id)))
            newRow
          })
        }
        elem.toIterator
      })

  val numColumns = hadoopRelation.schema.length
  val outCols = output.length

  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (sparkContext.getConf.getBoolean(key = SkipStringsKey, defaultValue = true))
      doExecuteColumnarByteArray()
    else
      doExecuteColumnarString()
  }

  protected def doExecuteColumnarByteArray(): RDD[ColumnarBatch] = {
    val evaluator = nativeEvaluator.forCode(CsvParse.CsvParseCode)
    val imfi = hadoopRelation.location.asInstanceOf[InMemoryFileIndex]
    sparkContext.wholeRawTextFiles(imfi.rootPaths.head.toString).map { case (name, text) =>
      transformRawTextFile(numColumns, outCols, evaluator, name, text)(logger)
    }
  }

  protected def doExecuteColumnarString(): RDD[ColumnarBatch] = {
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
      val outRows = outColumns.head.getValueCount

      val millis = System.currentTimeMillis() - startTime
      logInfo(s"Took ${millis} ms to process CSV: ${name} (${text.length} bytes)")
      val batch = new ColumnarBatch(outColumns.map(col => new ArrowColumnVector(col)).toArray)
      batch.setNumRows(outRows)
      batch
    }
  }

}
