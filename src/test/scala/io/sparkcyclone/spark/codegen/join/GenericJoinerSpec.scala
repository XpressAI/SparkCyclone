package io.sparkcyclone.spark.codegen.join

import io.sparkcyclone.native.code._
import io.sparkcyclone.spark.codegen.CFunctionGeneration.{CExpression, TypedCExpression2}
import io.sparkcyclone.spark.codegen.groupby.GroupByOutline.{GroupingKey, StagedAggregationAttribute, StagedProjection}
import io.sparkcyclone.spark.codegen.join.GenericJoiner.{FilteredOutput, Join}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.scalatest.freespec.AnyFreeSpec

class GenericJoinerSpec extends AnyFreeSpec {
  "Generic Joiner C Code works" in {

    val inputsLeft = List(CVector.int("foo"), CVector.int("bar"))
    val inputsRight = List(CVector.int("spam"), CVector.int("eggs"))
    val genericJoiner = GenericJoiner(
      inputsLeft = inputsLeft,
      inputsRight = inputsRight,
      joins = List(
        Join(inputsLeft(0), inputsRight(0)),
        Join(inputsLeft(1), inputsRight(1))
      ),
      outputs = List(
        FilteredOutput("outR", inputsRight(0)),
        FilteredOutput("outL", inputsLeft(0))
      )
    )

    val cFunction = genericJoiner
      .cFunction("join_function_name", "produces_indices")
      .definition

    println(cFunction.cCode)

    assert(cFunction != null)
  }
}
