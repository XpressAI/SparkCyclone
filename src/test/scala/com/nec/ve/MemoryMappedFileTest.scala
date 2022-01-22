package com.nec.ve

import io.mappedbus.MemoryMappedFile
import org.scalatest.freespec.AnyFreeSpec

import java.io.File

/** Including here, as this can only work on Linux. */
final class MemoryMappedFileTest extends AnyFreeSpec {

  val FILE_NAME = "/dev/shm/memorymappedfile-test"
  val FILE_SIZE = 1000L

  "It works" in {
    val fl = new File(FILE_NAME)

    fl.delete()
    try {
      val LIMIT = 0;
      val COMMIT = 8;
      val DATA = 16;
      val writer = new Thread() {
        override def run(): Unit = {
          try {
            val m = new MemoryMappedFile(FILE_NAME, FILE_SIZE)
            Thread.sleep(500)
            m.putLongVolatile(LIMIT, 1)
            Thread.sleep(500)
            m.putLong(DATA, 2)
            m.putLongVolatile(COMMIT, 1)
            m.unmap()
          } catch {
            case e: Throwable =>
              e.printStackTrace()
          }
        }
      }
      writer.start()

      try {
        val m = new MemoryMappedFile(FILE_NAME, FILE_SIZE)
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
        finally m.unmap()
      } finally {
        writer.join()
      }
    }
  }
}
