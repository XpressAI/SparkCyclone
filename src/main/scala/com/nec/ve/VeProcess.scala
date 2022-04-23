package com.nec.ve

import com.nec.colvector.{VeBatchOfBatches, VeColBatch, VeColVector, VeColVectorSource}
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.agile.core.{CScalarVector, CVarChar, CVector, VeString}
import com.nec.ve.VeProcess.Requires.requireOk
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp._
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.{veo_proc_handle, veo_thr_ctxt}

import java.io.{InputStream, OutputStream}
import java.nio.channels.Channels
import java.nio.file.Path
import scala.reflect.ClassTag

trait VeProcess {
  def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
    context: OriginalCallingContext
  ): Long
  def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit

  def validateVectors(list: Seq[VeColVector]): Unit
  def loadLibrary(path: Path): LibraryReference
  def allocate(size: Long)(implicit context: OriginalCallingContext): Long
  def putPointer(bytePointer: BytePointer)(implicit context: OriginalCallingContext): Long
  def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit

  /**
   * Asynchronously write a pointer to the given target location
   * @param source input
   * @param to target
   * @param context original calling context
   * @return Handle for checking the operation status or veo.VEO_REQUEST_ID_INVALID if the request failed
   */
  def putAsync(source: BytePointer, to: Long)(implicit context: OriginalCallingContext): Long

  /**
   * Asynchronously read from a pointer into the given destination
   * @param destination target (must have at least "size" capacity)
   * @param source source pointer on the VE
   * @param size count of bytes to read
   * @return Handle for checking the operation status or veo.VEO_REQUEST_ID_INVALID if the request failed
   */
  def getAsync(destination: BytePointer, source: Long, size: Long): Long

  /**
   * Check the result of an async operation without waiting for it to finish
   * @param handle handle for checking operation status
   * @param context original calling context
   * @return tuple of veo.VEO_COMMAND_OK | veo.VEO_COMMAND_EXCEPTION | veo.VEO_COMMAND_ERROR | veo.VEO_COMMAND_UNFINISHED | -1 (internal error) and the return value of the checked function
   */
  def peekResult(handle: Long)(implicit  context: OriginalCallingContext): (Int, Long)

  /**
   * Wait for the result of an async operation
   * @param handle handle for checking operation status
   * @param context original calling context
   * @return tuple of veo.VEO_COMMAND_OK | veo.VEO_COMMAND_EXCEPTION | veo.VEO_COMMAND_ERROR | veo.VEO_COMMAND_UNFINISHED | -1 (internal error) and the return value of the checked function
   */
  def waitResult(handle: Long)(implicit  context: OriginalCallingContext): (Int, Long)


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

    override def putAsync(bytePointer: BytePointer, to: Long)(implicit context: OriginalCallingContext): Long = f().putAsync(bytePointer, to)

    override def getAsync(destination: BytePointer, source: Long, size: Long): Long = f().getAsync(destination, source, size)

    override def peekResult(handle: Long)(implicit context: OriginalCallingContext): (Int, Long) = f().peekResult(handle)

    override def waitResult(handle: Long)(implicit context: OriginalCallingContext): (Int, Long) = f().waitResult(handle)

  }

  final case class WrappingVeo(
    veo_proc_handle: veo_proc_handle,
    veo_thr_ctxt: veo_thr_ctxt,
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
      val memoryLocation = allocate(bytePointer.limit())
      requireOk(
        veo.veo_write_mem(veo_proc_handle, memoryLocation, bytePointer, bytePointer.limit())
      )
      memoryLocation
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

      readAsyncAsVeColVector(readAllAsPointerAsync(outPointers.map(_.get()).zip(results)))
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
        set -> readAllAsPointerAsync(outPointers.map(_.get(set)).zip(results))
      }.map { case (set, asyncReads) =>
        set -> readAsyncAsVeColVector(asyncReads)
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

      readAsyncAsVeColVector(readAllAsPointerAsync(outPointers.map(_.get()).zip(results)))
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

      readAsyncAsVeColVector(readAllAsPointerAsync(outPointers.map(_.get()).zip(results)))
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

      val groups = VeColBatch(readAsyncAsVeColVector(readAllAsPointerAsync(List((groupsOutPointer.get(), results.head)))))
      val numGroups = groups.numRows
      val groupKeys = groups.toArray(0)(implicitly[ClassTag[K]], this)

      val scope = new PointerScope()

      val actualOutPointers = outPointers.map(_.get()).map { containerLocation =>
          val size = 8 * numGroups;
          val dst = new BytePointer(size)
          val handle = getAsync(dst, containerLocation, size)
          (dst, handle)
        }.map { case (dst, handle) =>
          require(waitResult(handle)._1 == veo.VEO_COMMAND_OK)
          new LongPointer(dst)
      }

      val o = (0 until numGroups).map { (i) =>
        val k: K = groupKeys(i)
        k -> readAllAsPointerAsync(actualOutPointers.map(_.get(i)).zip(results))
      }.map{ case (k, asyncReads) =>
        k -> readAsyncAsVeColVector(asyncReads)
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

    private def readAllAsPointerAsync(pointerVecs: List[(Long, CVector)]): List[(Long, BytePointer, Long, CVector)] = {
      pointerVecs.map {
        case (outContainerLocation, cvec) =>
          require(
            outContainerLocation > 0,
            s"Expected container location to be > 0, got ${outContainerLocation}"
          )

          val dest = new BytePointer(cvec.veType.containerSize)
          val handle = getAsync(dest, outContainerLocation, cvec.veType.containerSize)
          (outContainerLocation, dest, handle, cvec)
      }
    }

    private def readAsyncAsVeColVector(handles: List[(Long, BytePointer, Long, CVector)])(implicit
    context: OriginalCallingContext
    ): List[VeColVector] = {
      handles.map { case (outContainerLocation, bytePointer, handle, cvec) =>
        val (asyncResult, _) = waitResult(handle)
        require(asyncResult == veo.VEO_COMMAND_OK)

        val veColVector = cvec match {
          case CVarChar(name) =>
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
          case CScalarVector(name, r) =>
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
        bytePointer.close()
        veColVector
      }
    }

    override def putAsync(bytePointer: BytePointer, to: Long)(implicit context: OriginalCallingContext): Long = {
      veo.veo_async_write_mem(veo_thr_ctxt, to, bytePointer, bytePointer.limit())
    }

    override def getAsync(destination: BytePointer, source: Long, size: Long): Long = {
      veo.veo_async_read_mem(veo_thr_ctxt, destination, source, size)
    }

    override def peekResult(handle: Long)(implicit context: OriginalCallingContext): (Int, Long) = {
      val retp = new LongPointer(0)
      val res = veo.veo_call_peek_result(veo_thr_ctxt, handle, retp)
      val retVal = retp.get()
      requireOk(retVal.toInt)
      retp.close()
      (res, retVal)
    }

    override def waitResult(handle: Long)(implicit context: OriginalCallingContext): (Int, Long) = {
      val retp = new LongPointer(0)
      val res = veo.veo_call_wait_result(veo_thr_ctxt, handle, retp)
      val retVal = retp.get()
      requireOk(retVal.toInt)
      retp.close()
      (res, retVal)
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
