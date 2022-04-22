package com.nec.ve

import com.nec.colvector.{VeBatchOfBatches, VeColBatch, VeColVector, VeColVectorSource}
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.agile.core.{CScalarVector, CVarChar, CVector, VeString}
import com.nec.ve.VeProcess.Requires.requireOk
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp._
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.io.{InputStream, OutputStream}
import java.nio.channels.Channels
import java.nio.file.Path
import scala.reflect.ClassTag

trait VeProcess {
  def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
    context: OriginalCallingContext
  ): Long
  def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit

  final def readAsPointer(containerLocation: Long, containerSize: Int): BytePointer = {
    val bp = new BytePointer(containerSize)
    get(containerLocation, bp, containerSize)
    bp
  }

  def validateVectors(list: Seq[VeColVector]): Unit
  def loadLibrary(path: Path): LibraryReference
  def allocate(size: Long)(implicit context: OriginalCallingContext): Long
  def putPointer(bytePointer: BytePointer)(implicit context: OriginalCallingContext): Long
  def get(from: Long, to: BytePointer, size: Long): Unit
  def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit

  /** Return a single dataset */
  def execute(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector]

  /** Return multiple datasets - eg for sorting/exchanges */
  def executeMulti(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[(Int, List[VeColVector])]

  def executeMultiIn(
    libraryReference: LibraryReference,
    functionName: String,
    batches: VeBatchOfBatches,
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector]

  /**
   * Takes in multiple batches and returns multiple batches
   */
  def executeJoin(
    libraryReference: LibraryReference,
    functionName: String,
    left: VeBatchOfBatches,
    right: VeBatchOfBatches,
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[VeColVector]

  def executeGrouping[K: ClassTag](
    libraryReference: LibraryReference,
    functionName: String,
    inputs: VeBatchOfBatches,
    results: List[CVector]
  )(implicit context: OriginalCallingContext): List[(K, List[VeColVector])]
}

object VeProcess {
  var calls = 0
  var veSeconds = 0.0

  final case class OriginalCallingContext(fullName: sourcecode.FullName, line: sourcecode.Line) {
    def renderString: String = s"${fullName.value}#${line.value}"
  }

  object OriginalCallingContext {
    def make(implicit
      fullName: sourcecode.FullName,
      line: sourcecode.Line
    ): OriginalCallingContext =
      OriginalCallingContext(fullName, line)

    object Automatic {
      implicit def originalCallingContext(implicit
        fullName: sourcecode.FullName,
        line: sourcecode.Line
      ): OriginalCallingContext = make
    }
  }

  final case class LibraryReference(value: Long)
  final case class DeferredVeProcess(f: () => VeProcess) extends VeProcess with LazyLogging {
    override def validateVectors(list: Seq[VeColVector]): Unit = f().validateVectors(list)
    override def loadLibrary(path: Path): LibraryReference = f().loadLibrary(path)

    override def allocate(size: Long)(implicit context: OriginalCallingContext): Long =
      f().allocate(size)

    override def putPointer(bytePointer: BytePointer)(implicit
      context: OriginalCallingContext
    ): Long =
      f().putPointer(bytePointer)

    override def get(from: Long, to: BytePointer, size: Long): Unit = f().get(from, to, size)

    override def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit =
      f().free(memoryLocation)

    override def execute(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[VeColVector] =
      f().execute(libraryReference, functionName, cols, results)

    /** Return multiple datasets - eg for sorting/exchanges */
    override def executeMulti(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[(Int, List[VeColVector])] =
      f().executeMulti(libraryReference, functionName, cols, results)

    override def executeMultiIn(
      libraryReference: LibraryReference,
      functionName: String,
      batches: VeBatchOfBatches,
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[VeColVector] =
      f().executeMultiIn(libraryReference, functionName, batches, results)

    override def executeJoin(
       libraryReference: LibraryReference,
       functionName: String,
       left: VeBatchOfBatches,
       right: VeBatchOfBatches,
       results: List[CVector]
     )(implicit context: OriginalCallingContext): List[VeColVector] =
      f().executeJoin(libraryReference, functionName, left, right, results)

    override def executeGrouping[K: ClassTag](
      libraryReference: LibraryReference,
      functionName: String,
      inputs: VeBatchOfBatches,
      results: List[CVector]
    )(implicit context: OriginalCallingContext):  List[(K, List[VeColVector])] =
      f().executeGrouping(libraryReference, functionName, inputs, results)

    override def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit =
      f().writeToStream(outStream, bufPos, bufLen)

    override def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
      context: OriginalCallingContext
    ): Long = f().loadFromStream(inputStream, bytes)
  }

  final case class WrappingVeo(
    veo_proc_handle: veo_proc_handle,
    source: VeColVectorSource,
    veProcessMetrics: VeProcessMetrics
  ) extends VeProcess
    with LazyLogging {
    override def allocate(size: Long)(implicit context: OriginalCallingContext): Long = {
      val veInputPointer = new LongPointer(1)
      veo.veo_alloc_mem(veo_proc_handle, veInputPointer, size)
      val ptr = veInputPointer.get()
      logger.trace(
        s"Allocating ${size} bytes ==> ${ptr} in ${context.fullName.value}#${context.line.value}"
      )
      veProcessMetrics.registerAllocation(size, ptr)
      ptr
    }

    private implicit class RichVCV(veColVector: VeColVector) {
      def register()(implicit context: OriginalCallingContext): VeColVector = {
        veColVector.buffers.zip(veColVector.bufferSizes).foreach {
          case (location, size) =>
            logger.trace(
              s"Registering allocation of ${size} at ${location}; original source is ${context.fullName.value}#${context.line.value}"
            )
            veProcessMetrics.registerAllocation(size, location)
        }
        veColVector
      }
    }

    override def putPointer(
      bytePointer: BytePointer
    )(implicit context: OriginalCallingContext): Long = {
      val start = System.nanoTime()
      val memoryLocation = allocate(bytePointer.limit())
      requireOk(
        veo.veo_write_mem(veo_proc_handle, memoryLocation, bytePointer, bytePointer.limit())
      )
      val stop = System.nanoTime()
      val duration = (stop - start).asInstanceOf[Double] / 1000000
      val throughput = (bytePointer.limit() / 1024) / (duration / 1000)
      println(s"Putting pointer ${bytePointer.address()}; size ${bytePointer.limit()}; duration $duration ms; speed $throughput kb/s (Start: $start; Stop: $stop)")
      memoryLocation
    }

    override def get(from: Long, to: BytePointer, size: Long): Unit = {
      val start = System.nanoTime()
      veo.veo_read_mem(veo_proc_handle, to, from, size)
      val stop = System.nanoTime()
      val duration = (stop - start).asInstanceOf[Double] / 1000000
      val throughput = (size.asInstanceOf[Double] / 1024) / (duration / 1000)
      println(s"Reading pointer ${from}; size ${size}; duration $duration ms; speed $throughput kb/s (Start: $start; Stop: $stop)")
    }

    override def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit = {
      veProcessMetrics.deregisterAllocation(memoryLocation)
      logger.trace(
        s"Deallocating ptr ${memoryLocation} (in ${context.fullName.value}#${context.line.value})"
      )
      veo.veo_free_mem(veo_proc_handle, memoryLocation)
    }

    def validateVectors(list: Seq[VeColVector]): Unit = {
      list.foreach(vector =>
        require(
          vector.source == source,
          s"Expecting source to be ${source}, but got ${vector.source} for vector ${vector}"
        )
      )
    }

    override def execute(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[VeColVector] = {
      validateVectors(cols)
      val our_args = veo.veo_args_alloc()
      cols.zipWithIndex.foreach { case (vcv, index) =>
        val lp = new LongPointer(1)
        lp.put(vcv.container)
        veo.veo_args_set_stack(our_args, 0, index, new BytePointer(lp), 8)
      }
      val outPointers = results.map { veType =>
        val lp = new LongPointer(1)
        lp.put(-118)
        lp
      }
      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = reIdx + cols.size
        veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointers(reIdx)), 8)
      }
      val fnCallResult = new LongPointer(1)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

      require(
        functionAddr > 0,
        s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
      )

      logger.debug(s"[execute] Calling $functionName")
      val start = System.nanoTime()
      val callRes = veProcessMetrics.measureRunningTime(
        veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)
      )(veProcessMetrics.registerVeCall)
      val end = System.nanoTime()
      VeProcess.veSeconds += (end - start) / 1e9
      VeProcess.calls += 1
      logger.debug(
        s"Finished $functionName Calls: ${VeProcess.calls} VeSeconds: (${VeProcess.veSeconds} s)"
      )

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); args: $cols; returns $results"
      )
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      outPointers.zip(results).map {
        case (outPointer, CScalarVector(name, scalar)) =>
          val outContainerLocation = outPointer.get()
          val bytePointer = readAsPointer(outContainerLocation, scalar.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(16),
            name = name,
            veType = scalar,
            container = outContainerLocation,
            buffers = Seq(bytePointer.getLong(0), bytePointer.getLong(8)),
            dataSize = None
          ).register()
        case (outPointer, CVarChar(name)) =>
          val outContainerLocation = outPointer.get()
          val bytePointer = readAsPointer(outContainerLocation, VeString.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(36),
            name = name,
            dataSize = Some(bytePointer.getInt(32)),
            veType = VeString,
            container = outContainerLocation,
            buffers = Seq(
              bytePointer.getLong(0),
              bytePointer.getLong(8),
              bytePointer.getLong(16),
              bytePointer.getLong(24)
            )
          ).register()
      }
    }

    override def loadLibrary(path: Path): LibraryReference = {
      SparkCycloneExecutorPlugin.libsPerProcess
        .getOrElseUpdate(
          veo_proc_handle,
          scala.collection.mutable.Map.empty[String, LibraryReference]
        )
        .getOrElseUpdate(
          path.toString, {
            logger.info(s"Loading library from path ${path}...")
            val libRe = veo.veo_load_library(veo_proc_handle, path.toString)
            require(libRe > 0, s"Expected lib ref to be > 0, got ${libRe} (library at: ${path})")
            logger.info(s"Loaded library from ${path} as $libRe")
            LibraryReference(libRe)
          }
        )
    }

    /** Return multiple datasets - eg for sorting/exchanges */
    override def executeMulti(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[(Int, List[VeColVector])] = {

      validateVectors(cols)

      val MaxSetsCount = 64

      val our_args = veo.veo_args_alloc()
      cols.zipWithIndex.foreach { case (vcv, index) =>
        val lp = new LongPointer(1)
        lp.put(vcv.container)
        veo.veo_args_set_stack(our_args, 0, index, new BytePointer(lp), 8)
      }
      val outPointers = results.map { veType =>
        val lp = new LongPointer(MaxSetsCount)
        lp.put(-99)
        lp
      }
      val countsP = new IntPointer(4.toLong)
      countsP.put(-919)
      veo.veo_args_set_stack(our_args, 1, cols.size, new BytePointer(countsP), 8)
      results.zipWithIndex.zip(outPointers).foreach { case ((vet, reIdx), outPointer) =>
        val index = cols.size + 1 + reIdx
        veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointer), MaxSetsCount * 8)
      }
      val fnCallResult = new LongPointer(1)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

      require(
        functionAddr > 0,
        s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
      )

      logger.debug(s"[executeMulti] Calling $functionName")
      val start = System.nanoTime()
      val callRes = veProcessMetrics.measureRunningTime(
        veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)
      )(veProcessMetrics.registerVeCall)
      val end = System.nanoTime()
      VeProcess.veSeconds += (end - start) / 1e9
      VeProcess.calls += 1
      logger.debug(
        s"Finished $functionName Calls: ${VeProcess.calls} VeSeconds: (${VeProcess.veSeconds} s)"
      )

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); args: $cols"
      )
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      val gotCounts = countsP.get()
      require(
        gotCounts >= 0 && gotCounts <= MaxSetsCount,
        s"Expected 0 to $MaxSetsCount counts, got $gotCounts. Input args are ${cols}, results are $results"
      )

      (0 until gotCounts).toList.map { set =>
        set -> outPointers.zip(results).map {
          case (outPointer, CVarChar(name)) =>
            val outContainerLocation = outPointer.get(set)
            require(
              outContainerLocation > 0,
              s"Expected container location to be > 0, got ${outContainerLocation} for set ${set}"
            )
            val bytePointer = readAsPointer(outContainerLocation, VeString.containerSize)

            VeColVector(
              source = source,
              numItems = bytePointer.getInt(36),
              name = name,
              veType = VeString,
              container = outContainerLocation,
              buffers = Seq(
                bytePointer.getLong(0),
                bytePointer.getLong(8),
                bytePointer.getLong(16),
                bytePointer.getLong(24)
              ),
              dataSize = Some(bytePointer.getInt(32))
            ).register()
          case (outPointer, CScalarVector(name, r)) =>
            val outContainerLocation = outPointer.get(set)
            require(
              outContainerLocation > 0,
              s"Expected container location to be > 0, got ${outContainerLocation} for set ${set}"
            )
            val bytePointer = readAsPointer(outContainerLocation, r.containerSize)

            VeColVector(
              source = source,
              numItems = bytePointer.getInt(16),
              name = name,
              veType = r,
              container = outContainerLocation,
              buffers = Seq(bytePointer.getLong(0), bytePointer.getLong(8)),
              dataSize = None
            ).register()
        }
      }
    }

    override def executeMultiIn(
      libraryReference: LibraryReference,
      functionName: String,
      batches: VeBatchOfBatches,
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[VeColVector] = {

      batches.batches.foreach(batch => validateVectors(batch.columns))
      val our_args = veo.veo_args_alloc()

      /** Total batches count for input pointers */
      veo.veo_args_set_i32(our_args, 0, batches.batches.size)

      /** Output count of rows - better to know this in advance */
      veo.veo_args_set_i32(our_args, 1, batches.numRows)

      batches.groupedColumns.zipWithIndex.foreach { case (colGroup, index) =>
        val byteSize = 8 * batches.batches.size
        val lp = new LongPointer(batches.batches.size)
        colGroup.columns.zipWithIndex.foreach { case (col, idx) =>
          lp.put(idx, col.container)
        }
        veo.veo_args_set_stack(our_args, 0, 2 + index, new BytePointer(lp), byteSize)
      }
      val outPointers = results.map { veType =>
        val lp = new LongPointer(1)
        lp.put(-118)
        lp
      }
      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = 2 + batches.numColumns + reIdx
        veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointers(reIdx)), 8)
      }
      val fnCallResult = new LongPointer(1)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

      require(
        functionAddr > 0,
        s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
      )

      logger.debug(s"[executeMultiIn] Calling $functionName")
      val start = System.nanoTime()
      val callRes = veProcessMetrics.measureRunningTime(
        veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)
      )(veProcessMetrics.registerVeCall)
      val end = System.nanoTime()
      VeProcess.veSeconds += (end - start) / 1e9
      VeProcess.calls += 1
      logger.debug(
        s"Finished $functionName Calls: ${VeProcess.calls} VeSeconds: (${VeProcess.veSeconds} s)"
      )

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); args: $batches; returns $results"
      )
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      outPointers.zip(results).map {
        case (outPointer, CScalarVector(name, scalar)) =>
          val outContainerLocation = outPointer.get()
          val bytePointer = readAsPointer(outContainerLocation, scalar.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(16),
            name = name,
            veType = scalar,
            container = outContainerLocation,
            buffers = Seq(bytePointer.getLong(0), bytePointer.getLong(8)),
            dataSize = None
          ).register()
        case (outPointer, CVarChar(name)) =>
          val outContainerLocation = outPointer.get()
          val bytePointer = readAsPointer(outContainerLocation, VeString.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(36),
            name = name,
            dataSize = Some(bytePointer.getInt(32)),
            veType = VeString,
            container = outContainerLocation,
            buffers = Seq(
              bytePointer.getLong(0),
              bytePointer.getLong(8),
              bytePointer.getLong(16),
              bytePointer.getLong(24)
            )
          ).register()
      }
    }

    override def executeJoin(
       libraryReference: LibraryReference,
       functionName: String,
       left: VeBatchOfBatches,
       right: VeBatchOfBatches,
       results: List[CVector]
     )(implicit context: OriginalCallingContext): List[VeColVector] = {
      left.batches.foreach(batch => validateVectors(batch.columns))
      right.batches.foreach(batch => validateVectors(batch.columns))

      val our_args = veo.veo_args_alloc()

      val leftBatchSize = left.batches.size
      val leftColCount = left.numColumns
      val rightBatchSize = right.batches.size
      val rightColCount = right.numColumns

      val metaParamCount = 4

      /** Total batches count for left & right input pointers */
      veo.veo_args_set_u64(our_args, 0, leftBatchSize)
      veo.veo_args_set_u64(our_args, 1, rightBatchSize)

      /** Input count of rows - better to know this in advance */
      veo.veo_args_set_u64(our_args, 2, left.numRows)
      veo.veo_args_set_u64(our_args, 3, right.numRows)

      // Setup input pointers, such that each input pointer points to a batch of columns
      left.batches.head.columns.indices.foreach { cIdx =>
        val byteSize = 8 * leftBatchSize
        val lp = new LongPointer(leftBatchSize)

        left.batches.zipWithIndex.foreach{ case (b, bIdx) =>
          val col = b.columns(cIdx)
          lp.put(bIdx, col.container)
        }

        veo.veo_args_set_stack(our_args, 0, metaParamCount + cIdx, new BytePointer(lp), byteSize)
      }

      right.batches.head.columns.indices.foreach { cIdx =>
        val byteSize = 8 * rightBatchSize
        val lp = new LongPointer(rightBatchSize)

        right.batches.zipWithIndex.foreach{ case (b, bIdx) =>
          val col = b.columns(cIdx)
          lp.put(bIdx, col.container)
        }

        veo.veo_args_set_stack(our_args, 0, metaParamCount + leftColCount + cIdx, new BytePointer(lp), byteSize)
      }

      val outPointers = results.map { _ =>
        val lp = new LongPointer(1)
        lp.put(-118)
        lp
      }
      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = metaParamCount + leftColCount + rightColCount + reIdx
        veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointers(reIdx)), 8)
      }

      val fnCallResult = new LongPointer(1)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

      require(
        functionAddr > 0,
        s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
      )

      logger.debug(s"[executeJoin] Calling $functionName")
      val start = System.nanoTime()
      val callRes = veProcessMetrics.measureRunningTime(
        veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)
      )(veProcessMetrics.registerVeCall)
      val end = System.nanoTime()
      VeProcess.veSeconds += (end - start) / 1e9
      VeProcess.calls += 1
      logger.debug(
        s"Finished $functionName Calls: ${VeProcess.calls} VeSeconds: (${VeProcess.veSeconds} s)"
      )

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); left: $left; right: $right; returns $results"
      )
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      outPointers.zip(results).map {
        case (outPointer, CScalarVector(name, scalar)) =>
          val outContainerLocation = outPointer.get()
          val bytePointer = readAsPointer(outContainerLocation, scalar.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(16),
            name = name,
            veType = scalar,
            container = outContainerLocation,
            buffers = Seq(bytePointer.getLong(0), bytePointer.getLong(8)),
            dataSize = None
          ).register()
        case (outPointer, CVarChar(name)) =>
          val outContainerLocation = outPointer.get()
          val bytePointer = readAsPointer(outContainerLocation, VeString.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(36),
            name = name,
            dataSize = Some(bytePointer.getInt(32)),
            veType = VeString,
            container = outContainerLocation,
            buffers = Seq(
              bytePointer.getLong(0),
              bytePointer.getLong(8),
              bytePointer.getLong(16),
              bytePointer.getLong(24)
            )
          ).register()
      }
    }

    override def executeGrouping[K: ClassTag](
      libraryReference: LibraryReference,
      functionName: String,
      inputs: VeBatchOfBatches,
      results: List[CVector]
    )(implicit context: OriginalCallingContext): List[(K, List[VeColVector])] = {
      inputs.batches.foreach(batch => validateVectors(batch.columns))

      val our_args = veo.veo_args_alloc()

      val inputsBatchSize = inputs.batches.size
      val inputsColCount = inputs.numColumns

      val metaParamCount = 2

      veo.veo_args_set_u64(our_args, 0, inputsBatchSize)

      val groupsOutPointer = new LongPointer(1)
      veo.veo_args_set_stack(our_args, veo.VEO_INTENT_OUT, 1, new BytePointer(groupsOutPointer), groupsOutPointer.sizeof())

      // Setup input pointers, such that each input pointer points to a batch of columns
      inputs.batches.head.columns.indices.foreach { cIdx =>
        val byteSize = 8 * inputsBatchSize
        val lp = new LongPointer(inputsBatchSize)

        inputs.batches.zipWithIndex.foreach{ case (b, bIdx) =>
          val col = b.columns(cIdx)
          lp.put(bIdx, col.container)
        }

        veo.veo_args_set_stack(our_args, veo.VEO_INTENT_IN, metaParamCount + cIdx, new BytePointer(lp), byteSize)
      }

      val outPointers = results.map { veType =>
        val lp = new LongPointer(1)
        lp.put(-118)
        lp
      }

      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = metaParamCount + inputsColCount + reIdx
        veo.veo_args_set_stack(our_args, veo.VEO_INTENT_OUT, index, new BytePointer(outPointers(reIdx)), 8)
      }

      val fnCallResult = new LongPointer(1)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

      require(
        functionAddr > 0,
        s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
      )

      logger.debug(s"[executeGrouping] Calling $functionName")
      val start = System.nanoTime()
      val callRes = veProcessMetrics.measureRunningTime(
        veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)
      )(veProcessMetrics.registerVeCall)
      val end = System.nanoTime()
      VeProcess.veSeconds += (end - start) / 1e9
      VeProcess.calls += 1
      logger.debug(
        s"Finished $functionName Calls: ${VeProcess.calls} VeSeconds: (${VeProcess.veSeconds} s)"
      )

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); inputs: $inputs; returns $results"
      )


      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      val groups = VeColBatch(List(readVeColVector(groupsOutPointer.get(), results.head)))
      val numGroups = groups.numRows
      val groupKeys = groups.toArray(0)(implicitly[ClassTag[K]], this)

      val scope = new PointerScope()

      val actualOutPointers = outPointers.map(p => new LongPointer(readAsPointer(p.get(), 8 * numGroups)))

      val o = (0 until numGroups).map { (i) =>
        val k: K = groupKeys(i)

        k -> actualOutPointers.zip(results).map { case (outPointer, vector) =>
          val outContainerLocation = outPointer.get(i)
          val r = readVeColVector(outContainerLocation, vector)
          r
        }
      }.toList

      outPointers.foreach(p => veo.veo_free_mem(veo_proc_handle, p.get()))

      scope.close()
      o
    }

    override def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit = {
      if (bufLen > 1) {
        val buf = new BytePointer(bufLen)
        veo.veo_read_mem(veo_proc_handle, buf, bufPos, bufLen)
        val numWritten = Channels.newChannel(outStream).write(buf.asBuffer())
        require(numWritten == bufLen, s"Written ${numWritten}, expected ${bufLen}")
      }
    }

    override def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
      context: OriginalCallingContext
    ): Long = {
      val memoryLocation = allocate(bytes.toLong)
      val bp = new BytePointer(bytes.toLong)
      val buf = bp.asBuffer()

      val channel = Channels.newChannel(inputStream)
      var bytesRead = 0
      while (bytesRead < bytes) {
        bytesRead += channel.read(buf)
      }
      requireOk(
        veo.veo_write_mem(veo_proc_handle, memoryLocation, bp, bytes.toLong),
        s"Trying to write to memory location ${memoryLocation}; ${veProcessMetrics.checkTotalUsage()}"
      )
      memoryLocation
    }

    private def readVeColVector(outContainerLocation: Long, cVector: CVector)(implicit originalCallingContext: OriginalCallingContext): VeColVector = {
      cVector match {
        case CScalarVector(name, scalar) =>
          val bytePointer = readAsPointer(outContainerLocation, scalar.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(16),
            name = name,
            veType = scalar,
            container = outContainerLocation,
            buffers = Seq(bytePointer.getLong(0), bytePointer.getLong(8)),
            dataSize = None
          ).register()
        case CVarChar(name) =>
          val bytePointer = readAsPointer(outContainerLocation, VeString.containerSize)

          VeColVector(
            source = source,
            numItems = bytePointer.getInt(36),
            name = name,
            dataSize = Some(bytePointer.getInt(32)),
            veType = VeString,
            container = outContainerLocation,
            buffers = Seq(
              bytePointer.getLong(0),
              bytePointer.getLong(8),
              bytePointer.getLong(16),
              bytePointer.getLong(24)
            )
          ).register()
      }
    }

  }

  object Requires {
    def requireOk(result: Int): Unit = {
      require(result >= 0, s"Result should be >=0, got $result")
    }
    def requireOk(result: Int, extra: => String): Unit = {
      require(result >= 0, s"Result should be >=0, got $result; ${extra}")
    }

    def requirePositive(result: Long): Unit = {
      require(result > 0, s"Result should be > 0, got $result")
    }

    def requirePositive(result: Long, note: => String): Unit = {
      require(result > 0, s"Result should be > 0, got $result; $note")
    }
  }
}
