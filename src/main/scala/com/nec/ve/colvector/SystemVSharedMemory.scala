package com.nec.ve.colvector

import com.nec.ve.colvector.SystemVSharedMemory.PointerWrapper
import io.mappedbus.SharedMemory
import org.bytedeco.javacpp.{LongPointer, Pointer}
import org.bytedeco.systems.global.linux

import java.io.{File, FileOutputStream}

object SystemVSharedMemory {
  val DefaultShmPerms = 511 // 0777
  val Megabyte = 1024 * 1024

  def createSharedMemory(
    id: String,
    name: String,
    sizeInMb: Int,
    isFirst: Boolean
  ): SystemVSharedMemory = {
    val path = s"/tmp/cyclone-${name}"
    val file = new File(path)
    if (!file.exists()) {
      new FileOutputStream(file).close()
    }

    val systemVIPCKey = linux.ftok(path, 1)

    val shmSize = sizeInMb * Megabyte

    val shmId =
      linux.shmget(
        systemVIPCKey,
        shmSize,
        if (isFirst) (linux.IPC_CREAT | linux.SHM_HUGETLB | DefaultShmPerms) else linux.SHM_HUGETLB
      )

    if (shmId < 0) {
      throw new RuntimeException(
        s"Unable to get or create Shared Memory Segment at $path (result = ${shmId}, errno = ${linux.errno()})"
      )
    }

    val p = linux.shmat(shmId, null, 0)

    if (p.address() < 0)
      throw new RuntimeException("Unable to attach to shared memory.")

    new SystemVSharedMemory(id, shmId, new PointerWrapper(p))
  }

  class PointerWrapper(pointer: Pointer) extends Pointer(pointer) {
    def addr(): Long = address
  }
}

final class SystemVSharedMemory(id: String, val shmId: Int, pointer: PointerWrapper)
  extends SharedMemory {

  private val longPointer = new LongPointer(pointer)

  override def addr(): Long = pointer.addr()

  override def unmap(): Unit = {
    // Mark to be cleaned up automatically.
    linux.shmctl(shmId, linux.IPC_RMID, null)
    linux.shmdt(pointer)
  }

  override def getLong(pos: Long): Long =
    longPointer.get(pos)

  override def getLongVolatile(pos: Long): Long =
    longPointer.get(pos)

  override def putLong(pos: Long, value: Long): Unit =
    longPointer.put(pos, value)

  override def putLongVolatile(pos: Long, value: Long): Unit =
    longPointer.put(pos, value)

}
