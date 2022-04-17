package com.nec.colvector

import cats.effect.IO
import cats.effect.kernel.Resource
import org.apache.arrow.memory.RootAllocator

object WithTestAllocator {
  def apply[T](f: RootAllocator => T): T = {
    val alloc = new RootAllocator(Integer.MAX_VALUE)
    try f(alloc)
    finally {} //alloc.close()
  }

  def resource: Resource[IO, RootAllocator] = {
    Resource.eval(IO.delay(new RootAllocator(Integer.MAX_VALUE)))
  }
}
