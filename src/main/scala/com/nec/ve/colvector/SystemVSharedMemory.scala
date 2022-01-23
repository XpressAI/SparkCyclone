package com.nec.ve.colvector

import io.mappedbus.SharedMemory
import org.bytedeco.javacpp.{LongPointer, Pointer}
import org.bytedeco.systems.global.linux

import java.io.{File, FileOutputStream}

object SystemVSharedMemory {
  def createSharedMemory(id: String, name: String, sizeInMb: Int): SystemVSharedMemory = {
    val path = s"/tmp/cyclone-${name}"
    val file = new File(path)
    if (!file.exists()) {
      new FileOutputStream(file).close()
    }
    val key = linux.ftok(path, 1)

    val shmSize = sizeInMb * 1024 * 1024;
    val perms = 511 // 0777
    val shmId = if (id == "2") { // First executor.
      linux.shmget(key, shmSize, linux.IPC_CREAT | linux.SHM_HUGETLB | perms)
    } else {
      linux.shmget(key, shmSize, linux.SHM_HUGETLB)
    }
    if (shmId < 0) {
      throw new RuntimeException(s"Unable to get or create Shared Memory Segment at $path")
    }

    val p = linux.shmat(shmId, null, 0)
    if (p.address() == -1) {
      throw new RuntimeException("Unable to attach to shared memory.")
    }

    val longPointer = new LongPointer(p)
    longPointer.put(0, 1234);

    SystemVSharedMemory(id, shmId, p)
  }
}
case class SystemVSharedMemory(id: String, shmId: Int, addr: Pointer) extends SharedMemory {
  val longPointer = new LongPointer(addr)

  def unmap(): Unit = {
    // Mark to be cleaned up automatically.
    if (id == "2") {
      linux.shmctl(shmId, linux.IPC_RMID, null)
    }

    linux.shmdt(addr);
  }

  def getLong(pos: Long): Long = {
    longPointer.get(pos)
  }

  def getLongVolatile(pos: Long): Long = {
    longPointer.get(pos)
  }

  def putLong(pos: Long, value: Long): Unit = {
    longPointer.put(pos, value)
  }

  def putLongVolatile(pos: Long, value: Long): Unit = {
    longPointer.put(pos, value)
  }
}
