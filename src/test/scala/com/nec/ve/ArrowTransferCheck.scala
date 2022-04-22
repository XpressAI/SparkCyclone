package com.nec.ve

import com.nec.colvector.BytePointerColVector
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core.{VeNullableDouble, VeString}
import com.nec.spark.agile.exchange.GroupingFunction
import com.nec.spark.agile.merge.MergeFunction
import com.nec.ve.PureVeFunctions.{DoublingFunction, PartitioningFunction}
import com.nec.colvector.VeColBatch
import com.nec.colvector.VeBatchOfBatches
import com.nec.ve.VeProcess.OriginalCallingContext
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.freespec.AnyFreeSpec

@VectorEngineTest
final class ArrowTransferCheck extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  import OriginalCallingContext.Automatic._
  "Execute our function" in {
    compiledWithHeaders(DoublingFunction, "f") { path =>
      val lib = veProcess.loadLibrary(path)
      val colvec = Seq[Double](1, 2, 3).map(Some(_)).toBytePointerColVector("input").toVeColVector

      val outputs = veProcess.execute(
        libraryReference = lib,
        functionName = "f",
        cols = List(colvec),
        results = List(VeNullableDouble.makeCVector("output"))
      )

      outputs.map(_.toBytePointerColVector.toSeqOpt[Double].flatten) should be (Seq(Seq[Double](2, 4, 6)))
    }
  }

  "Execute multi-function" in {
    compiledWithHeaders(PartitioningFunction, "f") { path =>
      val lib = veProcess.loadLibrary(path)
      val colvec = Seq[Double](95, 99, 105, 500, 501).map(Some(_)).toBytePointerColVector("input").toVeColVector

      val outputs = veProcess.executeMulti(
        libraryReference = lib,
        functionName = "f",
        cols = List(colvec),
        results = List(VeNullableDouble.makeCVector("output"))
      )

      val results: List[(Int, Option[Double])] = outputs.map { case (index, vecs) =>
        index -> {
          val tmp = vecs.head.toBytePointerColVector.toSeqOpt[Double].flatten
          if (tmp.isEmpty) None else Some(tmp.max)
        }
      }

      val expected = List(
        (0, Some(99.0)),
        (1, Some(105.0)),
        (2, None),
        (3, None),
        (4, Some(501.0))
      )

      results should be (expected)
    }
  }

  "Partition data by some means (simple Int partitioning in this case) (PIN)" in {
    val groupingFn = GroupingFunction(
      "f",
      List(
        GroupingFunction.DataDescription(VeNullableDouble, GroupingFunction.Key),
        GroupingFunction.DataDescription(VeString, GroupingFunction.Key),
        GroupingFunction.DataDescription(VeNullableDouble, GroupingFunction.Value)
      ),
      2
    )

    compiledWithHeaders(groupingFn.toCFunction) { path =>
      val lib = veProcess.loadLibrary(path)

      val lastString = "cccc"
      val colvec1 = Seq[Double](1, 2, 3).map(Some(_)).toBytePointerColVector("1").toVeColVector
      val colvec2 = Seq[Double](9, 8, 7).map(Some(_)).toBytePointerColVector("2").toVeColVector
      val colvecS = Seq("a", "b", lastString).map(Some(_)).toBytePointerColVector("3").toVeColVector

      val outputs = veProcess.executeMulti(
        libraryReference = lib,
        functionName = groupingFn.name,
        cols = List(colvec1, colvecS, colvec2),
        results = List(VeNullableDouble, VeString, VeNullableDouble).zipWithIndex
          .map { case (vt, i) => vt.makeCVector(s"out_${i}") }
      )

      val results: List[(Int, List[(Double, String, Double)])] = outputs.map { case (index, vecs) =>
        index -> (
          vecs(0).toBytePointerColVector.toSeqOpt[Double].flatten,
          vecs(1).toBytePointerColVector.toSeqOpt[String].flatten,
          vecs(2).toBytePointerColVector.toSeqOpt[Double].flatten,
        ).zipped.toList
      }

      results.map(_._2.size).toSet == Set(1, 2)
      results.flatMap(_._2).toSet should be (Set[(Double, String, Double)]((1, "a", 9), (2, "b", 8), (3, lastString, 7)))
    }
  }

  /**
   * Let's first take the data, as it is,
   * perform partial aggregation,
   * then bucket it,
   * then exchange it,
   * re-merge according to buckets
   * then finalize
   */

  "We can merge multiple VeColBatches" in {
    val mergeFn = MergeFunction("merger", List(VeNullableDouble, VeString))

    compiledWithHeaders(mergeFn.toCFunction) { path =>
      val lib = veProcess.loadLibrary(path)

      val colvec1 = Seq[Double](1, 2, 3, -1).map(Some(_)).toBytePointerColVector("1").toVeColVector
      val colvec2 = Seq[Double](2, 3, 4).map(Some(_)).toBytePointerColVector("2").toVeColVector
      val svec1 = Seq("a", "b", "c", "x").map(Some(_)).toBytePointerColVector("3").toVeColVector
      val svec2 = Seq("d", "e", "f").map(Some(_)).toBytePointerColVector("4").toVeColVector

      val colbatch1 = VeColBatch(Seq(colvec1, svec1))
      val colbatch2 = VeColBatch(Seq(colvec2, svec2))
      val batches = VeBatchOfBatches(List(colbatch1, colbatch2))

      val outputs = veProcess.executeMultiIn(
        libraryReference = lib,
        functionName = mergeFn.name,
        batches = batches,
        results = colbatch1.columns.zipWithIndex.map { case (vcv, idx) =>
          vcv.veType.makeCVector(s"o_${idx}")
        }.toList
      )

      outputs.size should be (2)
      outputs(0).toBytePointerColVector.toSeqOpt[Double].flatten should be (Seq[Double](1, 2, 3, -1, 2, 3, 4))
      outputs(1).toBytePointerColVector.toSeqOpt[String].flatten should be (Seq("a", "b", "c", "x", "d", "e", "f"))
    }
  }
}
