package com.nec.ve

import com.nec.ve.colvector.SystemVSharedMemory
import io.mappedbus.{MemoryMappedFile, SharedMemory}
import org.scalatest.freespec.AnyFreeSpec

import java.io.File

/** Including here, as this can only work on Linux. */
final class MemoryMappedFileTest extends AnyFreeSpec {

  val FILE_NAME_SHM = "/dev/shm/memorymappedfile-test"
  val FILE_NAME_SYSV = "/tmp/sysv"
  val FILE_SIZE = 1000L

  def testFlow(writerM: => SharedMemory, readerM: => SharedMemory): Unit = {
    val wM: SharedMemory = writerM
    wM.putLong(0, 9)
    val rM: SharedMemory = readerM
    try {
      val res = rM.getLong(0)
      assert(res == 9)
    } finally {
      rM.unmap()
      wM.unmap()
    }
  }

  "It works with SHM" in {
    val fl = new File(FILE_NAME_SHM)

    fl.delete()

    testFlow(
      writerM = new MemoryMappedFile(FILE_NAME_SHM, FILE_SIZE),
      readerM = new MemoryMappedFile(FILE_NAME_SHM, FILE_SIZE)
    )
  }
  "It works with SystemV" in {
    testFlow(
      writerM = SystemVSharedMemory.createSharedMemory("x", "y", 1024, isFirst = true),
      readerM = SystemVSharedMemory.createSharedMemory("x", "y", 1024, isFirst = false)
    )
  }
}
