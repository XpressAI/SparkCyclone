package com.nec.spark.planning
import com.nec.arrow.ArrowNativeInterfaceNumeric
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
import com.nec.native.IpcTransfer
import com.nec.native.NativeEvaluator
import com.nec.spark.planning.NativeCsvExec.SkipStringsKey
import com.nec.spark.planning.NativeCsvExec.UseIpc
import com.nec.spark.planning.NativeCsvExec.transformLazyDataStream
import com.nec.spark.planning.NativeCsvExec.transformRawTextFile
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.SparkContext
import org.apache.spark.WholeTextFileRawRDD.RichSparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.io.DataInputStream
import java.io.InputStream
import java.nio.ByteBuffer

object NativeCsvExec {
  case class NativeCsvStrategy(nativeEvaluator: NativeEvaluator) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      ScanOperation
        .unapply(plan)
        .collect {
          case i @ (a, b, LogicalRelation(rel: HadoopFsRelation, out, cat, iss))
              if rel.fileFormat.isInstanceOf[CSVFileFormat] =>
            NativeCsvExec(
              hadoopRelation = rel,
              output = a.map(_.toAttribute),
              nativeEvaluator = nativeEvaluator
            )
        }
        .orElse {
          PartialFunction.condOpt(plan) {
            case lr @ LogicalRelation(rel: HadoopFsRelation, out, cat, iss)
                if rel.fileFormat.isInstanceOf[CSVFileFormat] =>
              NativeCsvExec(hadoopRelation = rel, output = out, nativeEvaluator = nativeEvaluator)
          }
        }
        .toList
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
    val millis = System.currentTimeMillis() - startTime
    logger.info(s"Took ${millis} ms to process CSV: ${name} (${text.getLength} bytes)")
    new ColumnarBatch(
      outColumns.map(col => new ArrowColumnVector(col)).toArray,
      outColumns.head.getValueCount
    )
  }

  val bufSize = 16 * 1024

  def transformInputStream(
    numColumns: Int,
    outCols: Int,
    evaluator: ArrowNativeInterfaceNumeric,
    name: String,
    inputStream: InputStream
  )(implicit logger: Logger): ColumnarBatch = {
    val allocator = ArrowUtilsExposed.rootAllocator
      .newChildAllocator(s"CSV read allocator", 0, Long.MaxValue)

    val outColumns = (0 until numColumns).map { idx =>
      new Float8Vector(s"out_${idx}", allocator)
    }.toList
    val startTime = System.currentTimeMillis()
    logger.debug(s"Beginning transfer process..")

    val (socketPath, serverSocket) = IpcTransfer.transferIPC(inputStream, bufSize)

    try evaluator.callFunction(
      name = if (numColumns == 3) "parse_csv_ipc" else s"parse_csv_${numColumns}_ipc",
      inputArguments = List(Some(StringWrapper(socketPath))) ++ outColumns.map(_ => None),
      outputArguments = List(None) ++ outColumns.map(col => Some(Float8VectorWrapper(col)))
    )
    finally serverSocket.close()
    val millis = System.currentTimeMillis() - startTime
    logger.debug(s"Took ${millis} ms to process CSV: ${name}")
    // need to dealloc() the ignored columns here
    new ColumnarBatch(
      outColumns.map(col => new ArrowColumnVector(col)).toArray,
      outColumns.head.getValueCount
    )
  }

  def maybeDecodePds(
    name: String,
    hadoopConfiguration: Configuration,
    portableDataStream: PortableDataStream
  ): InputStream = {
    val original = portableDataStream.open()
    val theCodec =
      new CompressionCodecFactory(hadoopConfiguration).getCodec(new Path(name))
    if (theCodec != null) new DataInputStream(theCodec.createInputStream(original))
    else original
  }

  def transformLazyDataStream(
    numColumns: Int,
    outCols: Int,
    evaluator: ArrowNativeInterfaceNumeric,
    name: String,
    portableDataStream: PortableDataStream,
    hadoopConfiguration: Configuration
  )(implicit logger: Logger): ColumnarBatch = {
    logger.debug("Will use portable data stream transfer...")
    val startTime = System.currentTimeMillis()

    val columnarBatch =
      transformInputStream(
        numColumns,
        outCols,
        evaluator,
        name,
        maybeDecodePds(name, hadoopConfiguration, portableDataStream)
      )
    val endTime = System.currentTimeMillis()
    logger.debug(s"Took ${endTime - startTime}ms")
    columnarBatch
  }

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

  val numColumns = hadoopRelation.schema.length
  val outCols = output.length

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (
      sparkContext.getConf.getBoolean(
        key = UseIpc,
        defaultValue = true
      ) && !scala.util.Properties.isWin
    )
      doExecuteColumnarIPC()
    else if (sparkContext.getConf.getBoolean(key = SkipStringsKey, defaultValue = true))
      doExecuteColumnarByteArray()
    else
      doExecuteColumnarString()
  }

  protected def doExecuteColumnarIPC(): RDD[ColumnarBatch] = {
    val evaluator = nativeEvaluator.forCode(CsvParse.CsvParseCode)
    val imfi = hadoopRelation.location.asInstanceOf[InMemoryFileIndex]
    sparkContext
      .binaryFiles(imfi.rootPaths.head.toString)
      .map { case (name, pds) =>
        transformLazyDataStream(
          numColumns,
          outCols,
          evaluator,
          name,
          pds,
          SparkContext.getOrCreate().hadoopConfiguration
        )(logger)
      }
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
      val millis = System.currentTimeMillis() - startTime
      logInfo(s"Took ${millis} ms to process CSV: ${name} (${text.length} bytes)")
      new ColumnarBatch(
        outColumns.map(col => new ArrowColumnVector(col)).toArray,
        outColumns.head.getValueCount
      )
    }
  }

}
