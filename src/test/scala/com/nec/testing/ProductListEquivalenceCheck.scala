package com.nec.testing
import ProductListEquivalenceCheck._
import com.eed3si9n.expecty.Expecty.expect
import com.nec.cmake.DynamicCSqlExpressionEvaluationSpec
import com.nec.spark.SparkAdditions
import com.nec.spark.agile.CFunctionGeneration.CFunction
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalactic.source.Position
import org.scalactic.{Equality, Equivalence, TolerantNumerics}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAllConfigMap, ConfigMap}
import scalatags.Text.tags2.{details, summary}

final class ProductListEquivalenceCheck extends AnyFreeSpec {
  "A list of classes with some Doubles is equivalent" in {
    assert(
      listEq.areEqual(List[(String, Double)](("a", 0.0005)), List[(String, Double)](("a", 0.0003)))
    )
  }
  "When values are too far apart, it is no longer equivalent" in {
    assert(
      !listEq.areEqual(List[(String, Double)](("a", 1.0005)), List[(String, Double)](("a", 0.0003)))
    )
  }
}

object ProductListEquivalenceCheck {
  class ContainsTheSameElementsAsWithDoubleTolerance[A  <: Product](expected: Seq[A]) extends Matcher[Seq[A]] {
    def apply(left: Seq[A]): MatchResult = {
      val notEqual = left.zipAll(expected, null, null).filter {
        case (elem, expect) if(elem == null || expect == null) => true
        case (elem: Product, expect: Product) => !twoProductsEq.areEqual(elem, expect)
      }
      val message = notEqual.map{
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

  def shouldContainTheSameProducts[A <: Product](expected: Seq[A]) = new ContainsTheSameElementsAsWithDoubleTolerance[A](expected)

  implicit val doubleEq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-2)
  implicit val twoProductsEq: Equality[Product] = (aProduct: Product, _b: Any) => {
    val bProduct = _b.asInstanceOf[Product]
    aProduct.productArity == bProduct.productArity &&
    aProduct.productIterator.zip(bProduct.productIterator).forall {
      case (a: Double, b: Double) =>
        doubleEq.areEquivalent(a, b)
      case (a, b) =>
        a == b
    }
  }
  implicit val listEq: Equality[List[Product]] = (a: List[Product], _b: Any) => {
    val b = _b.asInstanceOf[List[Product]]
    (a.isEmpty && b.isEmpty) || ((a.size == b.size) && a.zip(b).forall {
      case (aProduct, bProduct) =>
        twoProductsEq.areEqual(aProduct, bProduct)
    })
  }
}
