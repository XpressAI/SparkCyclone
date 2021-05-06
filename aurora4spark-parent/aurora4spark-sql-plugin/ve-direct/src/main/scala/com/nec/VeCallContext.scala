package com.nec

import com.nec.VeCallContext.{Argument, IntArgument, ListDoubleArgument}
import com.nec.VeFunction.StackArgument
import me.shadaj.scalapy.py
import sun.misc.Unsafe

final case class VeCallContext(
  veo: py.Dynamic,
  lib: py.Dynamic,
  ctxt: py.Dynamic,
  unsafe: Unsafe,
  np: py.Dynamic
) {

  /**
   * TODO pass a Java-created pointer rather than a Python-created pointer. This requires direct
   * AVEO API from the JVM
   *
   * This is all the key boilerplate to minimize the size of these function definitions
   */
  def execute[T](
    veFunction: VeFunction[StackArgument],
    uploadData: VeFunction[Option[Long]] => Unit,
    loadData: (py.Dynamic, VeFunction[Option[Long]]) => T,
    arguments: Seq[Argument]
  ): T = {

    val argumentPointers = arguments.map{
      case ListDoubleArgument(size) => Some(np.zeros(size, dtype = np.double))
      case IntArgument(value) => None
    }
    val numPyBits = veFunction.copy(args = argumentPointers.toList)

    val rawDataPositions = numPyBits.map {
      _.map { npArr =>
        npArr.__array_interface__.bracketAccess("data").bracketAccess(0).as[Long]
      }
    }

    uploadData(rawDataPositions)
    val args: List[py.Any] = ctxt :: numPyBits.args.zip(arguments).map {
      case (Some(npv), ListDoubleArgument(size)) => veo.OnStack(npv, inout = veo.INTENT_INOUT): py.Any
      case (None, IntArgument(value))      => value: py.Any
    }
    val req = lib.applyDynamic(veFunction.name)(args: _*)
    val res = req.wait_result()

    loadData(res, rawDataPositions)

  }
}

object VeCallContext {
  sealed trait Argument
  case class IntArgument(value: Int) extends Argument
  case class ListDoubleArgument(size: Int) extends Argument
}

