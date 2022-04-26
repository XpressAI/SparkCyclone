package com.nec.ve

import com.nec.ve.VeProcess.OriginalCallingContext
import org.bytedeco.veoffload.global.veo

class VeAsyncResult[T](
  val process: VeProcess,
  val handles: Seq[Long],
  val fn: () => T)(
  implicit val originalCallingContext: OriginalCallingContext
) {
  lazy private val result: T = {
    handles.foreach{ handle =>
      require(process.waitResult(handle)._1 == veo.VEO_COMMAND_OK)
    }

    fn()
  }

  def get(): T = result

  def map[U](g: T => U): VeAsyncResult[U] = {
    new VeAsyncResult[U](
      process,
      handles,
      () => {g(fn())}
    )
  }

  def flatMap[U](g: T => VeAsyncResult[U]): VeAsyncResult[U] = {
    new VeAsyncResult[U](
      process,
      handles,
      () => {g(fn()).get()}
    )
  }
}

object VeAsyncResult {
  def apply[T](handle: Long)(fn: () => T)(implicit process: VeProcess, originalCallingContext: OriginalCallingContext): VeAsyncResult[T] = {
    new VeAsyncResult[T](process, Seq(handle), fn)
  }

  def apply[T](handles: Seq[Long])(fn: () => T)(implicit process: VeProcess, originalCallingContext: OriginalCallingContext): VeAsyncResult[T] = {
    new VeAsyncResult[T](process, handles, fn)
  }

  def empty()(implicit process: VeProcess, originalCallingContext: OriginalCallingContext): VeAsyncResult[Unit] = VeAsyncResult(Nil){ () =>}
}
