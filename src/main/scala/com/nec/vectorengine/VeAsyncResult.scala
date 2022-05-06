package com.nec.vectorengine

final class VeAsyncResult[T](handles: Seq[VeAsyncReqId],
                             thunk: () => T)
                            (implicit process: VeProcess) {
  // Only fetch once, since calling await twice will return error from the VE
  private lazy val result: T = {
    handles.foreach { id =>
      val retp = process.awaitResult(id)
      // All transfers and Cyclone C++ functions return 0 on success
      require(retp.get == 0, s"Expected asynchronous function call to return 0; got ${retp.get}")
    }
    thunk()
  }

  def get: T = {
    result
  }

  def map[U](g: T => U): VeAsyncResult[U] = {
    new VeAsyncResult[U](
      handles,
      () => { g(thunk()) }
    )
  }

  def flatMap[U](g: T => VeAsyncResult[U]): VeAsyncResult[U] = {
    new VeAsyncResult[U](
      handles,
      () => { g(thunk()).get }
    )
  }
}

object VeAsyncResult {
  def apply[T](handles: VeAsyncReqId*)
              (thunk: () => T)
              (implicit process: VeProcess): VeAsyncResult[T] = {
    new VeAsyncResult[T](handles, thunk)
  }

  def empty(implicit process: VeProcess): VeAsyncResult[Unit] = {
    new VeAsyncResult[Unit](Nil, { () => })
  }
}
