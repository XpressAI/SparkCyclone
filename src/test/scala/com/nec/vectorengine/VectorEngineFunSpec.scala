package com.nec.vectorengine

import com.nec.colvector._
import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.spark.agile.core._
import com.nec.spark.agile.exchange.GroupingFunction
import com.nec.spark.agile.merge.MergeFunction
import com.nec.util.CallContextOps._
import com.nec.ve.VeKernelInfra
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VectorEngineFunSpec extends AnyWordSpec with WithVeProcess with VeKernelInfra {
  "VectorEngine" should {
    "correctly execute a basic VE function [1] (single input, single output)" in {
      val func = SampleVeFunctions.DoublingFunction

      compiledWithHeaders(func) { path =>
        val lib = process.load(path)

        val inputs = InputSamples.seqOpt[Double](Random.nextInt(20) + 10)
        val colvec = inputs.toBytePointerColVector("_").toVeColVector2

        val outputs = engine.execute(
          lib,
          func.name,
          inputs = Seq(colvec),
          outputs = Seq(VeNullableDouble.makeCVector("output"))
        )

        val expected = inputs.map(_.map(_ * 2))
        outputs.map(_.toBytePointerColVector2.toSeqOpt[Double]) should be (Seq(expected))
      }
    }

    "correctly execute a basic VE function [2] (multi input, multi output)" in {
      val func = SampleVeFunctions.FilterEvensFunction

      compiledWithHeaders(func) { path =>
        val lib = process.load(path)

        val inputs0 = InputSamples.array[Int](Random.nextInt(20) + 10)
        val inputs1 = InputSamples.seqOpt[Double](inputs0.size)
        val inputs2 = InputSamples.seqOpt[Double](inputs0.size)

        val colvec0 = inputs0.toBytePointerColVector("_").toVeColVector2
        val colvec1 = inputs1.toBytePointerColVector("_").toVeColVector2
        val colvec2 = inputs2.toBytePointerColVector("_").toVeColVector2

        val outputs = engine.execute(
          lib,
          func.name,
          inputs = Seq(colvec0, colvec1, colvec2),
          outputs = Seq(VeNullableDouble.makeCVector("output1"), VeNullableDouble.makeCVector("output2"))
        )

        val expected1 = (inputs0, inputs1).zipped.map { case (i0, i1) =>
          if (i0 % 2 == 0) i1 else None
        }.toSeq
        val expected2 = (inputs0, inputs2).zipped.map { case (i0, i2) =>
          if (i0 % 2 == 0) i2 else None
        }.toSeq

        outputs.map(_.toBytePointerColVector2.toSeqOpt[Double]) should be (Seq(expected1, expected2))
      }
    }

    "correctly execute a multi-function [1] (simple partitioning)" in {
      val func = SampleVeFunctions.PartitioningFunction

      compiledWithHeaders(func) { path =>
        val lib = process.load(path)
        val colvec = Seq[Double](95, 99, 105, 500, 501).map(Some(_)).toBytePointerColVector("_").toVeColVector2

        val outputs = engine.executeMulti(
          lib,
          func.name,
          inputs = Seq(colvec),
          outputs = Seq(VeNullableDouble.makeCVector("output"))
        )

        val results: Seq[(Int, Option[Double])] = outputs.map { case (index, vecs) =>
          index -> {
            val tmp = vecs.head.toBytePointerColVector2.toSeqOpt[Double].flatten
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

    "correctly execute a multi-function [2] (custom grouping function)" in {
      val groupingFn = GroupingFunction(
        "grouping_func",
        List(
          GroupingFunction.DataDescription(VeNullableDouble, GroupingFunction.Key),
          GroupingFunction.DataDescription(VeString, GroupingFunction.Key),
          GroupingFunction.DataDescription(VeNullableDouble, GroupingFunction.Value)
        ),
        2
      )

      compiledWithHeaders(groupingFn.toCFunction) { path =>
        val lib = process.load(path)

        val lastString = "cccc"
        val colvec1 = Seq[Double](1, 2, 3).map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val colvec2 = Seq[Double](9, 8, 7).map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val colvecS = Seq("a", "b", lastString).map(Some(_)).toBytePointerColVector("_").toVeColVector2

        val outputs = engine.executeMulti(
          lib,
          groupingFn.name,
          inputs = Seq(colvec1, colvecS, colvec2),
          outputs = Seq(VeNullableDouble, VeString, VeNullableDouble).zipWithIndex
            .map { case (vt, i) => vt.makeCVector(s"out_${i}") }
        )

        val results: Seq[(Int, Seq[(Double, String, Double)])] = outputs.map { case (index, vecs) =>
          index -> (
            vecs(0).toBytePointerColVector2.toSeqOpt[Double].flatten,
            vecs(1).toBytePointerColVector2.toSeqOpt[String].flatten,
            vecs(2).toBytePointerColVector2.toSeqOpt[Double].flatten,
          ).zipped.toSeq
        }

        results.map(_._2.size).toSet == Set(1, 2)
        results.flatMap(_._2).toSet should be (Set[(Double, String, Double)]((1, "a", 9), (2, "b", 8), (3, lastString, 7)))
      }
    }

    "correctly execute a multi-in function [1] (merge multiple VeColBatches)" in {
      val mergeFn = MergeFunction("merge_func", List(VeNullableDouble, VeString))

      compiledWithHeaders(mergeFn.toCFunction) { path =>
        val lib = process.load(path)

        val colvec1 = Seq[Double](1, 2, 3, -1).map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val colvec2 = Seq[Double](2, 3, 4).map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val svec1 = Seq("a", "b", "c", "x").map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val svec2 = Seq("d", "e", "f").map(Some(_)).toBytePointerColVector("_").toVeColVector2

        val colbatch1 = VeColBatch(Seq(colvec1, svec1))
        val colbatch2 = VeColBatch(Seq(colvec2, svec2))
        val batches = VeBatchOfBatches(List(colbatch1, colbatch2))

        val outputs = engine.executeMultiIn(
          lib,
          mergeFn.name,
          inputs = batches,
          outputs = colbatch1.columns.zipWithIndex
            .map { case (vcv, idx) => vcv.veType.makeCVector(s"o_${idx}") }
        )

        outputs.size should be (2)
        outputs(0).toBytePointerColVector2.toSeqOpt[Double].flatten should be (Seq[Double](1, 2, 3, -1, 2, 3, 4))
        outputs(1).toBytePointerColVector2.toSeqOpt[String].flatten should be (Seq("a", "b", "c", "x", "d", "e", "f"))
      }
    }
  }
}
