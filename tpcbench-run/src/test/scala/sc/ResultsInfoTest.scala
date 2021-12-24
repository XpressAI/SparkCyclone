package sc

import org.scalatest.freespec.AnyFreeSpec

import scala.collection.immutable

final class ResultsInfoTest extends AnyFreeSpec {
  "We can reorder" in {
    val list: List[List[Option[AnyRef]]] = List(List[Option[Integer]](Some(1), Some(2), Some(3)))
    val nr =
      ResultsInfo(columns = List("x", "y", "z"), data = list)
    val ll = nr.reorder(List("z", "y"))
    assert(ll.columns == List("z", "y", "x"))
    val list1: List[List[Option[AnyRef]]] = List(List[Option[Integer]](Some(3), Some(2), Some(1)))
    assert(ll.data == list1)
  }
}
