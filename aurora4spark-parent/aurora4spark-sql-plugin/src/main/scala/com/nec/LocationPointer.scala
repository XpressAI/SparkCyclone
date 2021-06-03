package com.nec

import org.bytedeco.javacpp.Pointer

class LocationPointer(_addr: Long, _count: Long) extends Pointer {
  this.address = _addr
  this.limit = _count
  this.capacity = _count
}
