package com.nec.arrow
import org.apache.arrow.memory.RootAllocator

object WithTestAllocator {
  def apply[T](f: RootAllocator => T): T = {
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    try f(alloc)
    finally alloc.close()
  }
}
