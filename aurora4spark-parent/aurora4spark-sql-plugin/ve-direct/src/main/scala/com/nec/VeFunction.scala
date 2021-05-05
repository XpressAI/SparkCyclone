package com.nec

final case class VeFunction[A](name: String, args: List[A], ret_type: Option[String]) {
  def map[B](f: A => B): VeFunction[B] = copy(args = args.map(f))
}

object VeFunction {
  sealed abstract class StackArgument(val tpe: String)

  object StackArgument {
    case object ListOfDouble extends StackArgument("b'double*'")
    case object Int extends StackArgument("'int'")
  }
}
