package com.nec.ve

import com.nec.arrow.VeArrowNativeInterface.requireOk
import com.nec.spark.SparkCycloneExecutorPlugin
import com.nec.spark.SparkCycloneExecutorPlugin.metrics.{measureRunningTime, registerVeCall}
import com.nec.spark.agile.CFunctionGeneration.{CScalarVector, CVarChar, CVector, VeString}
import com.nec.ve.VeColBatch.{VeBatchOfBatches, VeColVector, VeColVectorSource}
import com.nec.ve.VeProcess.{LibraryReference, OriginalCallingContext}
import com.typesafe.scalalogging.LazyLogging
import org.bytedeco.javacpp.{BytePointer, IntPointer, LongPointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.file.Path

private[ve] final case class VeProcessVeo(
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
      veColVector.bufferLocations.zip(veColVector.underlying.bufferSizes).foreach {
        case (location, size) =>
          logger.trace(
            s"Registering allocation of ${size} at ${location}; original source is ${context.fullName.value}#${context.line.value}"
          )
          veProcessMetrics.registerAllocation(size, location)
      }
      veColVector
    }
  }

  override def putBuffer(byteBuffer: ByteBuffer)(implicit context: OriginalCallingContext): Long = {
    val memoryLocation = allocate(byteBuffer.capacity().toLong)
    requireOk(
      veo.veo_write_mem(
        veo_proc_handle,
        memoryLocation,
        new org.bytedeco.javacpp.Pointer(byteBuffer),
        byteBuffer.capacity().toLong
      )
    )
    memoryLocation
  }

  override def get(from: Long, to: ByteBuffer, size: Long): Unit =
    veo.veo_read_mem(veo_proc_handle, new org.bytedeco.javacpp.Pointer(to), from, size)

  override def free(memoryLocation: Long)(implicit context: OriginalCallingContext): Unit = {
    veProcessMetrics.deregisterAllocation(memoryLocation)
    logger.trace(
      s"Deallocating ptr ${memoryLocation} (in ${context.fullName.value}#${context.line.value})"
    )
    veo.veo_free_mem(veo_proc_handle, memoryLocation)
  }

  def validateVectors(list: List[VeColVector]): Unit = {
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
      lp.put(vcv.containerLocation)
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

    val functionAddr = measureRunningTime(
      veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)
    )(registerVeCall)

    require(
      functionAddr > 0,
      s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
    )

    val callRes = veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)

    require(
      callRes == 0,
      s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); args: $cols; returns $results"
    )
    require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

    outPointers.zip(results).map {
      case (outPointer, CScalarVector(name, scalar)) =>
        val outContainerLocation = outPointer.get()
        val byteBuffer = readAsBuffer(outContainerLocation, scalar.containerSize)

        VeColVector(
          source = source,
          numItems = byteBuffer.getInt(16),
          name = name,
          veType = scalar,
          containerLocation = outContainerLocation,
          bufferLocations = List(byteBuffer.getLong(0), byteBuffer.getLong(8)),
          variableSize = None
        ).register()
      case (outPointer, CVarChar(name)) =>
        val outContainerLocation = outPointer.get()
        val byteBuffer = readAsBuffer(outContainerLocation, VeString.containerSize)

        VeColVector(
          source = source,
          numItems = byteBuffer.getInt(28),
          name = name,
          variableSize = Some(byteBuffer.getInt(24)),
          veType = VeString,
          containerLocation = outContainerLocation,
          bufferLocations =
            List(byteBuffer.getLong(0), byteBuffer.getLong(8), byteBuffer.getLong(16))
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
      lp.put(vcv.containerLocation)
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
    val callRes = veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)

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
          val byteBuffer = readAsBuffer(outContainerLocation, VeString.containerSize)

          VeColVector(
            source = source,
            numItems = byteBuffer.getInt(28),
            name = name,
            veType = VeString,
            containerLocation = outContainerLocation,
            bufferLocations =
              List(byteBuffer.getLong(0), byteBuffer.getLong(8), byteBuffer.getLong(16)),
            variableSize = Some(byteBuffer.getInt(24))
          ).register()
        case (outPointer, CScalarVector(name, r)) =>
          val outContainerLocation = outPointer.get(set)
          require(
            outContainerLocation > 0,
            s"Expected container location to be > 0, got ${outContainerLocation} for set ${set}"
          )
          val byteBuffer = readAsBuffer(outContainerLocation, r.containerSize)

          VeColVector(
            source = source,
            numItems = byteBuffer.getInt(16),
            name = name,
            veType = r,
            containerLocation = outContainerLocation,
            bufferLocations = List(byteBuffer.getLong(0), byteBuffer.getLong(8)),
            variableSize = None
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

    batches.batches.foreach(batch => validateVectors(batch.cols))
    val our_args = veo.veo_args_alloc()

    /** Total batches count for input pointers */
    veo.veo_args_set_i32(our_args, 0, batches.batches.size)

    /** Output count of rows - better to know this in advance */
    veo.veo_args_set_i32(our_args, 1, batches.rows)

    batches.groupedColumns.zipWithIndex.foreach { case (colGroup, index) =>
      val byteSize = 8 * batches.batches.size
      val lp = new LongPointer(batches.batches.size)
      colGroup.relatedColumns.zipWithIndex.foreach { case (col, idx) =>
        lp.put(idx, col.containerLocation)
      }
      veo.veo_args_set_stack(our_args, 0, 2 + index, new BytePointer(lp), byteSize)
    }
    val outPointers = results.map { veType =>
      val lp = new LongPointer(1)
      lp.put(-118)
      lp
    }
    results.zipWithIndex.foreach { case (vet, reIdx) =>
      val index = 2 + batches.cols + reIdx
      veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointers(reIdx)), 8)
    }
    val fnCallResult = new LongPointer(1)

    val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

    require(
      functionAddr > 0,
      s"Expected > 0, but got ${functionAddr} when looking up function '${functionName}' in $libraryReference"
    )

    val callRes = veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)

    require(
      callRes == 0,
      s"Expected 0, got $callRes; means VE call failed for function $functionAddr ($functionName); args: $batches; returns $results"
    )
    require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

    outPointers.zip(results).map {
      case (outPointer, CScalarVector(name, scalar)) =>
        val outContainerLocation = outPointer.get()
        val byteBuffer = readAsBuffer(outContainerLocation, scalar.containerSize)

        VeColVector(
          source = source,
          numItems = byteBuffer.getInt(16),
          name = name,
          veType = scalar,
          containerLocation = outContainerLocation,
          bufferLocations = List(byteBuffer.getLong(0), byteBuffer.getLong(8)),
          variableSize = None
        ).register()
      case (outPointer, CVarChar(name)) =>
        val outContainerLocation = outPointer.get()
        val byteBuffer = readAsBuffer(outContainerLocation, VeString.containerSize)

        VeColVector(
          source = source,
          numItems = byteBuffer.getInt(28),
          name = name,
          variableSize = Some(byteBuffer.getInt(24)),
          veType = VeString,
          containerLocation = outContainerLocation,
          bufferLocations =
            List(byteBuffer.getLong(0), byteBuffer.getLong(8), byteBuffer.getLong(16))
        ).register()
    }
  }

  override def writeToStream(outStream: OutputStream, bufPos: Long, bufLen: Int): Unit = {
    val buf = new BytePointer(bufLen)
    veo.veo_read_mem(veo_proc_handle, buf, bufPos, bufLen)
    buf.position(0)
    var i = 0
    while (i < bufLen) {
      outStream.write(buf.get(i))
      i = i + 1
    }
  }

  override def loadFromStream(inputStream: InputStream, bytes: Int)(implicit
    context: OriginalCallingContext
  ): Long = {
    val memoryLocation = allocate(bytes.toLong)
    val bp = new BytePointer(bytes.toLong)
    var i = 0
    while (i < bytes) {
      val r: Byte = inputStream.read().toByte
      bp.put(i, r)
      i = i + 1
    }
    requireOk(veo.veo_write_mem(veo_proc_handle, memoryLocation, bp, bytes.toLong))
    memoryLocation
  }
}
