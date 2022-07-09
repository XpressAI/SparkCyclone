package io.sparkcyclone.spark.codegen.join

import io.sparkcyclone.native.code.CodeLines
import io.sparkcyclone.spark.codegen.join.GenericJoiner.EqualityPairing
import io.sparkcyclone.spark.codegen.join.JoinByEquality.Conjunction
import org.scalatest.freespec.AnyFreeSpec

final class JoinByEqualitySpec extends AnyFreeSpec {
  "If there is only 1 join condition, then we just pass the data back out" ignore {
    fail("Not done")
  }
  val condition2 = "(index_t1_a[i] == index_t1_b[j]) && (index_t2_a[i] == index_t2_b[j])"
  val conj2 = Conjunction(
    List(EqualityPairing("index_t1_a", "index_t2_a"), EqualityPairing("index_t1_b", "index_t2_b"))
  )
  s"If there are 2 join conditions, then we compare ${condition2}" in {
    val cond = conj2.condition
    assert(cond == condition2)
  }

  "Conj 2 has 2 loops" in {
    assert(
      conj2.loops == List(
        "int i = 0; i < index_t1_a.size(); i++",
        "int j = 0; j < index_t1_b.size(); j++"
      )
    )
  }

  val condition3 =
    "(index_t1_a[i] == index_t1_b[j] && index_t1_b[j] == index_t1_c[k]) && (index_t2_a[i] == index_t2_b[j] && index_t2_b[j] == index_t2_c[k])"
  val conj3 = Conjunction(
    List(
      EqualityPairing("index_t1_a", "index_t2_a"),
      EqualityPairing("index_t1_b", "index_t2_b"),
      EqualityPairing("index_t1_c", "index_t2_c")
    )
  )
  s"If there are 3 join conditions, then we compare ${condition3}" in {
    val cond = conj3.condition
    assert(cond == condition3)
  }

  "Loops can be nested" in {
    val r = JoinByEquality.nestLoop(List("a", "b"), CodeLines.from("z")).cCode
    val ex = CodeLines
      .from(
        "for ( a ) {",
        CodeLines.from("for ( b ) {", CodeLines.from("z").indented, "}").indented,
        "}"
      )
      .cCode

    assert(r == ex)
  }

}
