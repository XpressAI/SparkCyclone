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
    try {
      val LIMIT = 0;
      val COMMIT = 8;
      val DATA = 16;
      var wM: SharedMemory = null
      val writer = new Thread() {
        override def run(): Unit = {
          try {
            val m: SharedMemory = writerM
            wM = m
            Thread.sleep(500)
            m.putLongVolatile(LIMIT, 1)
            Thread.sleep(500)
            m.putLong(DATA, 2)
            m.putLongVolatile(COMMIT, 1)
          } catch {
            case e: Throwable =>
              e.printStackTrace()
          }
        }
      }
      writer.start()

      try {
        val m: SharedMemory = readerM
        var limit = m.getLong(LIMIT)
        assert(limit == 0)
        var break = false
        while (!break) {
          limit = m.getLongVolatile(LIMIT)
          if (limit != 0) {
            assert(limit == 1)
            break = true
          }
        }
        var commit = m.getLongVolatile(COMMIT)
        var data = m.getLong(DATA)
        assert(commit == 0)
        assert(data == 0)
        break = false
        while (!break) {
          commit = m.getLongVolatile(COMMIT)
          if (commit != 0) {
            assert(commit == 1)
            break = true
          }
        }
        data = m.getLong(DATA)
        try assert(data == 2)
        finally {
          m.unmap()
          wM.unmap()
        }
      } finally {
        writer.join()
      }
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
