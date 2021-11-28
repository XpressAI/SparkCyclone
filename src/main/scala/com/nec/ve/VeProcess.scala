package com.nec.ve

import com.nec.arrow.VeArrowNativeInterface.requireOk
import com.nec.spark.agile.CFunctionGeneration.{CFunction, VeScalarType, VeString, VeType}
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.LibraryReference
import org.bytedeco.javacpp.{BytePointer, IntPointer, LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.nio.ByteBuffer
import java.nio.file.Path
import java.nio.ByteOrder

trait VeProcess {
  final def readAsBuffer(containerLocation: Long, containerSize: Int): ByteBuffer = {
    val bb = ByteBuffer.allocateDirect(containerSize)
    get(containerLocation, bb, containerSize)
    bb
  }
  def loadLibrary(path: Path): LibraryReference
  def allocate(size: Long): Long
  def putBuffer(byteBuffer: ByteBuffer): Long
  def get(from: Long, to: ByteBuffer, size: Long): Unit
  def free(memoryLocation: Long): Unit

  /** Return a single dataset */
  def execute(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[VeType]
  ): List[VeColVector]

  /** Return multiple datasets - eg for sorting/exchanges */
  def executeMulti(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[VeType]
  ): List[(Int, List[VeColVector])]
}

object VeProcess {
  final case class LibraryReference(value: Long)
  final case class DeferredVeProcess(f: () => VeProcess) extends VeProcess {
    override def loadLibrary(path: Path): LibraryReference = f().loadLibrary(path)

    override def allocate(size: Long): Long = f().allocate(size)

    override def putBuffer(byteBuffer: ByteBuffer): Long = f().putBuffer(byteBuffer)

    override def get(from: Long, to: ByteBuffer, size: Long): Unit = f().get(from, to, size)

    override def free(memoryLocation: Long): Unit = f().free(memoryLocation)

    override def execute(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[VeType]
    ): List[VeColVector] =
      f().execute(libraryReference, functionName, cols, results)

    /** Return multiple datasets - eg for sorting/exchanges */
    override def executeMulti(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[VeType]
    ): List[(Int, List[VeColVector])] =
      f().executeMulti(libraryReference, functionName, cols, results)
  }
  final case class WrappingVeo(veo_proc_handle: veo_proc_handle) extends VeProcess {
    override def allocate(size: Long): Long = {
      val veInputPointer = new LongPointer(8)
      veo.veo_alloc_mem(veo_proc_handle, veInputPointer, size)
      veInputPointer.get()
    }

    override def putBuffer(byteBuffer: ByteBuffer): Long = {
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

    override def free(memoryLocation: Long): Unit =
      veo.veo_free_mem(veo_proc_handle, memoryLocation)

    override def execute(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[VeType]
    ): List[VeColVector] = {
      val our_args = veo.veo_args_alloc()
      cols.zipWithIndex.foreach { case (vcv, index) =>
        val lp = new LongPointer(8)
        lp.put(vcv.containerLocation)
        veo.veo_args_set_stack(our_args, 0, index, new BytePointer(lp), 8)
      }
      val outPointers = results.map { veType =>
        val lp = new LongPointer(8)
        lp.put(-118)
        lp
      }
      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = reIdx + cols.size
        veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointers(reIdx)), 8)
      }
      val fnCallResult = new LongPointer(8)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)

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
        case (outPointer, scalar: VeScalarType) =>
          val outContainerLocation = outPointer.get()
          val byteBuffer = readAsBuffer(outContainerLocation, scalar.containerSize)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

          VeColVector(
            numItems = byteBuffer.getInt(16),
            veType = scalar,
            containerLocation = outContainerLocation,
            bufferLocations = List(byteBuffer.getLong(0), byteBuffer.getLong(8))
          )
        case (outPointer, VeString) =>
          val outContainerLocation = outPointer.get()
          val byteBuffer = readAsBuffer(outContainerLocation, VeString.containerSize)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

          VeColVector(
            numItems = byteBuffer.getInt(28),
            veType = VeString,
            containerLocation = outContainerLocation,
            bufferLocations =
              List(byteBuffer.getLong(0), byteBuffer.getLong(8), byteBuffer.getLong(16))
          )
      }
    }

    override def loadLibrary(path: Path): LibraryReference = {
      val libRe = veo.veo_load_library(veo_proc_handle, path.toString)
      require(libRe > 0, s"Expected lib ref to be > 0, got ${libRe}")
      LibraryReference(libRe)
    }

    /** Return multiple datasets - eg for sorting/exchanges */
    override def executeMulti(
      libraryReference: LibraryReference,
      functionName: String,
      cols: List[VeColVector],
      results: List[VeType]
    ): List[(Int, List[VeColVector])] = {

      val MaxSetsCount = 64

      val our_args = veo.veo_args_alloc()
      cols.zipWithIndex.foreach { case (vcv, index) =>
        val lp = new LongPointer(8)
        lp.put(vcv.containerLocation)
        veo.veo_args_set_stack(our_args, 0, index, new BytePointer(lp), 8)
      }
      val outPointers = results.map { veType =>
        val lp = new LongPointer(8 * MaxSetsCount)
        lp.put(-99)
        lp
      }
      val countsP = new IntPointer(4.toLong)
      veo.veo_args_set_stack(our_args, 1, cols.size, new BytePointer(countsP), 4)
      results.zipWithIndex.zip(outPointers).foreach { case ((vet, reIdx), outPointer) =>
        val index = cols.size + 1 + reIdx
        veo.veo_args_set_stack(our_args, 1, index, new BytePointer(outPointer), MaxSetsCount * 8)
      }
      val fnCallResult = new LongPointer(8)

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
        gotCounts >= 0 && gotCounts < MaxSetsCount,
        s"Expected 0 to $MaxSetsCount counts, got $gotCounts"
      )

      (0 until gotCounts).toList.map { set =>
        set -> outPointers.zip(results).map { case (outPointer, r) =>
          val outContainerLocation = outPointer.get(set)
          require(
            outContainerLocation > 0,
            s"Expected container location to be > 0, got ${outContainerLocation} for set ${set}"
          )
          val byteBuffer = readAsBuffer(outContainerLocation, r.containerSize)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

          VeColVector(
            numItems = byteBuffer.getInt(16),
            veType = r,
            containerLocation = outContainerLocation,
            bufferLocations = List(byteBuffer.getLong(0), byteBuffer.getLong(8))
          )
        }
      }
    }

  }
}
