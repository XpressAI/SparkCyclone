package com.nec.ve

import com.nec.LocationPointer
import com.nec.arrow.VeArrowNativeInterface.requireOk
import com.nec.spark.agile.CFunctionGeneration.VeType
import com.nec.ve.VeColBatch.VeColVector
import com.nec.ve.VeProcess.LibraryReference
import org.bytedeco.javacpp.{BytePointer, LongPointer, Pointer}
import org.bytedeco.veoffload.global.veo
import org.bytedeco.veoffload.veo_proc_handle

import java.nio.ByteBuffer
import java.nio.file.Path

trait VeProcess {
  def readAsBuffer(containerLocation: Long, containerSize: Int): ByteBuffer = {
    val bb = ByteBuffer.allocateDirect(containerSize)
    get(containerLocation, bb, containerSize)
    bb
  }
  def loadLibrary(path: Path): LibraryReference
  def allocate(size: Long): Long
  def putBuffer(byteBuffer: ByteBuffer): Long
  def get(from: Long, to: ByteBuffer, size: Long): Unit
  def free(memoryLocation: Long): Unit
  def execute(
    libraryReference: LibraryReference,
    functionName: String,
    cols: List[VeColVector],
    results: List[VeType]
  ): List[VeColVector]
}

object VeProcess {
  final case class LibraryReference(value: Long)
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
        veo.veo_args_set_stack(
          our_args,
          0,
          index,
          new BytePointer(new LocationPointer(vcv.containerLocation, 8)),
          vcv.containerSize
        )
      }
      val outContainers = results.map { veType =>
        allocate(veType.containerSize)
      }
      results.zipWithIndex.foreach { case (vet, reIdx) =>
        val index = reIdx + cols.size
        veo.veo_args_set_stack(
          our_args,
          1,
          index,
          new BytePointer(new LocationPointer(outContainers(reIdx), 8)),
          vet.containerSize
        )
      }
      val fnCallResult = new LongPointer(8)

      val functionAddr = veo.veo_get_sym(veo_proc_handle, libraryReference.value, functionName)
      val callRes = veo.veo_call_sync(veo_proc_handle, functionAddr, our_args, fnCallResult)

      require(
        callRes == 0,
        s"Expected 0, got $callRes; means VE call failed for function $functionAddr; args: $cols"
      )
      require(fnCallResult.get() == 0L, s"Expected 0, got ${fnCallResult.get()} back instead.")

      outContainers.zip(results).map { case (outContainerLocation, r) =>
        val byteBuffer = readAsBuffer(outContainerLocation, r.containerSize)

        VeColVector(
          numItems = byteBuffer.getInt(byteBuffer.getInt(8)),
          veType = r,
          containerLocation = outContainerLocation,
          bufferLocations = List(byteBuffer.getInt(16), byteBuffer.getInt(8))
        )
      }
    }

    override def loadLibrary(path: Path): LibraryReference =
      LibraryReference(veo.veo_load_library(veo_proc_handle, path.toString))
  }
}
