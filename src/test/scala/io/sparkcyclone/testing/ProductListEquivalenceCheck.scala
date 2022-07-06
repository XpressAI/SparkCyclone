package io.sparkcyclone.testing

import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.matchers.should.Matchers._

final class ProductListEquivalenceCheckUnitSpec extends AnyWordSpec {
  import ProductListEquivalenceCheck._

  "SeqProductMatcherWithDoubleTolerance" should {
    "match when Doubles are close enough" in {
      listEq.areEqual(Seq(("a", 0.0005d)), Seq(("a", 0.0003d))) should be (true)
      Seq(("a", 0.0005d)) should containTheSameProducts(Seq(("a", 0.0003d)))
    }

    "not match when Doubles are too far apart" in {
      listEq.areEqual(Seq(("a", 1.0005d)), Seq(("a", 0.0003d))) should be (false)
      Seq(("a", 1.0005d)) shouldNot containTheSameProducts(Seq(("a", 0.0003d)))
    }
  }
}

object ProductListEquivalenceCheck {
  class SeqProductMatcherWithDoubleTolerance[A <: Product](expected: Seq[A]) extends Matcher[Seq[A]] {
    def apply(left: Seq[A]): MatchResult = {
      val notEqual = left.zipAll(expected, null, null).filter {
        case (elem, expect) if (elem == null || expect == null) => true
        case (elem: Product, expect: Product) => !productEq.areEqual(elem, expect)
      }

      val message = notEqual.map {
        case (null, expect) => s"Expected ${expect} but value was missing."
        case (value, null) => s"Value was ${value}, but didn't expect anything."
        case (value, expect) => s"Value was ${value}, expected was ${expect}."
      }.mkString("\n")

      MatchResult(
        notEqual.isEmpty,
        s"Sequences do not match. The list of mismatches: ${message}",
        "Sequences match."
      )
    }
  }

  def containTheSameProducts[A <: Product](expected: Seq[A]) = new SeqProductMatcherWithDoubleTolerance[A](expected)

  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-2)

  implicit val productEq: Equality[Product] = (aProduct: Product, _b: Any) => {
    val bProduct = _b.asInstanceOf[Product]
    aProduct.productArity == bProduct.productArity &&
    aProduct.productIterator.zip(bProduct.productIterator).forall {
      case (a: Double, b: Double) =>
        doubleEq.areEquivalent(a, b)
      case (a, b) =>
        a == b
    }
  }

  implicit val listEq: Equality[Seq[Product]] = (a: Seq[Product], _b: Any) => {
    val b = _b.asInstanceOf[Seq[Product]]
    (a.isEmpty && b.isEmpty) || ((a.size == b.size) && a.zip(b).forall {
      case (aProduct, bProduct) =>
        productEq.areEqual(aProduct, bProduct)
    })
  }
}
