package com.nec.spark.planning
import java.io.{DataInputStream, InputStream}
import java.nio.ByteBuffer

import scala.collection.JavaConversions.asScalaIterator

import com.nec.arrow.ArrowNativeInterfaceNumeric
import com.nec.arrow.ArrowNativeInterfaceNumeric.SupportedVectorWrapper._
import com.nec.arrow.functions.CsvParse
import com.nec.native.{IpcTransfer, NativeEvaluator}
import com.nec.spark.planning.NativeCsvExec.{SkipStringsKey, UseIpc, transformLazyDataStream, transformRawTextFile}
import com.nec.spark.planning.SparkPortingUtils.ScanOperation
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.arrow.vector.Float8Vector
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.WholeTextFileRawRDD.RichSparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, Divide, Multiply, Subtract, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.util.ArrowUtilsExposed
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

object NativeCsvExec {
  case class NativeCsvStrategy(nativeEvaluator: NativeEvaluator) extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val d = plan match {
      case Project(a, LogicalRelation(rel: HadoopFsRelation, out, cat, iss))
        if rel.fileFormat.isInstanceOf[CSVFileFormat] && isNotAggProjection(plan) =>
        Option(NativeCsvExec(
          hadoopRelation = rel,
          output = a.map(_.toAttribute),
          nativeEvaluator = nativeEvaluator
        ))

      case lr @ LogicalRelation(rel: HadoopFsRelation, out, cat, iss)
          if rel.fileFormat.isInstanceOf[CSVFileFormat] =>
          Option(NativeCsvExec(hadoopRelation = rel, output = out, nativeEvaluator = nativeEvaluator))

      case _ => None

    }
      d.toSeq
    }
  }

  private def isNotAggProjection(logicalPlan: LogicalPlan): Boolean = {
  logicalPlan match {
    case Project(projectList, child) => {
     projectList.collect{
       case Alias(Add(_, _), name) =>
       case Alias(Subtract(_, _), name) =>
       case Alias(Multiply(_, _), name) =>
       case Alias(Divide(_, _), name) =>
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
    val batch = new ColumnarBatch(
      outColumns.map(col => new ArrowColumnVector(col)).toArray
    )
    batch.setNumRows(outRows)
    batch
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
    val outRows = outColumns.head.getValueCount
    logger.debug(s"Took ${millis} ms to process CSV: ${name}")
    // need to dealloc() the ignored columns here
    val batch = new ColumnarBatch(
      outColumns.map(col => new ArrowColumnVector(col)).toArray
    )

    batch.setNumRows(outRows)
    batch
  }

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

  def transformLazyDataStream(
    numColumns: Int,
    outCols: Int,
    evaluator: ArrowNativeInterfaceNumeric,
    name: String,
    portableDataStream: PortableDataStream,
    hadoopConfiguration: SerializableConfiguration
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
      elem.foreach(row => (0 until row.numFields).foreach(id => print(s"ROW AUT: ${row.getDouble(id)}")))

      elem.toIterator.foreach(row => (0 until row.numFields).foreach(id => print(s"ROW OUT: ${row.getDouble(id)}")))
      println("")
      elem.toIterator
    })

  val numColumns = hadoopRelation.schema.length
  val outCols = output.length

  protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
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
    val hadoopConf = new SerializableConfiguration(sparkContext.hadoopConfiguration)

    sparkContext
      .binaryFiles(imfi.rootPaths.head.toString)
      .map { case (name, pds) =>
        transformLazyDataStream(
          numColumns,
          outCols,
          evaluator,
          name,
          pds,
          hadoopConf
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
      val outRows = outColumns.head.getValueCount

      val millis = System.currentTimeMillis() - startTime
      logInfo(s"Took ${millis} ms to process CSV: ${name} (${text.length} bytes)")
      val batch = new ColumnarBatch(
        outColumns.map(col => new ArrowColumnVector(col)).toArray)
      batch.setNumRows(outRows)
      batch
    }
  }

}
