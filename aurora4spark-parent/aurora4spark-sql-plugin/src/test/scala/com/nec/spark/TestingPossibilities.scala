package com.nec.spark
import org.scalatest.freespec.AnyFreeSpec

object TestingPossibilities {
  final case class Testing(name: String, verify: () => Unit, benchmark: () => Unit)
  val possibilities: List[Testing] = List(
    Testing("3Equals3", verify = () => assert(3 == 3), benchmark = () => ()),
    Testing("2Equals2", verify = () => assert(2 == 2), benchmark = () => ())
  )
}
final class TestingPossibilities extends AnyFreeSpec {
  TestingPossibilities.possibilities.foreach(testing => testing.name in testing.verify())
}
