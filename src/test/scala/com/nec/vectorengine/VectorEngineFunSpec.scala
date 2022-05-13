package com.nec.vectorengine

import com.nec.colvector._
import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.CppTranspiler
import com.nec.spark.agile.core._
import com.nec.spark.agile.exchange.GroupingFunction
import com.nec.spark.agile.join.SimpleEquiJoinFunction
import com.nec.spark.agile.merge.MergeFunction
import com.nec.util.CallContextOps._
import com.nec.ve.VeKernelInfra
import scala.reflect.runtime.universe._
import scala.util.Random
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

@VectorEngineTest
final class VectorEngineFunSpec extends AnyWordSpec with WithVeProcess with VeKernelInfra {
  "VeProcess" should {
    "be able to load the Cyclone C++ Library" in {
      noException should be thrownBy {
        val lib = process.load(LibCyclonePath)
      }
    }
  }

  "VectorEngine" should {
    "correctly execute a basic VE function [1] (single input, single output)" in {
      val func = SampleVeFunctions.DoublingFunction

      compiledWithHeaders(func) { path =>
        val lib = process.load(path)

        val inputs = InputSamples.seqOpt[Double](Random.nextInt(20) + 10)
        val colvec = inputs.toBytePointerColVector("_").toVeColVector2

        // Metrics should be empty
        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${func.name}") should be (null)

        val outputs = engine.execute(
          lib,
          func.name,
          inputs = Seq(colvec),
          outputs = Seq(VeNullableDouble.makeCVector("output"))
        )

        // Metrics should be non-empty
        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${func.name}").getCount should be (1L)

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

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${func.name}") should be (null)

        val outputs = engine.executeMulti(
          lib,
          func.name,
          inputs = Seq(colvec),
          outputs = Seq(VeNullableDouble.makeCVector("output"))
        )

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${func.name}").getCount should be (1L)

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

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${groupingFn.name}") should be (null)

        val outputs = engine.executeMulti(
          lib,
          groupingFn.name,
          inputs = Seq(colvec1, colvecS, colvec2),
          outputs = Seq(VeNullableDouble, VeString, VeNullableDouble).zipWithIndex
            .map { case (vt, i) => vt.makeCVector(s"out_${i}") }
        )

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${groupingFn.name}").getCount should be (1L)

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
      val mergeFn = MergeFunction("merge_func", Seq(VeNullableDouble, VeString))

      compiledWithHeaders(mergeFn.toCFunction) { path =>
        val lib = process.load(path)

        val colvec1 = Seq[Double](1, 2, 3, -1).map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val colvec2 = Seq[Double](2, 3, 4).map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val svec1 = Seq("a", "b", "c", "x").map(Some(_)).toBytePointerColVector("_").toVeColVector2
        val svec2 = Seq("d", "e", "f").map(Some(_)).toBytePointerColVector("_").toVeColVector2

        val colbatch1 = VeColBatch(Seq(colvec1, svec1))
        val colbatch2 = VeColBatch(Seq(colvec2, svec2))
        val batches = VeBatchOfBatches(Seq(colbatch1, colbatch2))

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${mergeFn.name}") should be (null)

        val outputs = engine.executeMultiIn(
          lib,
          mergeFn.name,
          inputs = batches,
          outputs = colbatch1.columns.zipWithIndex
            .map { case (vcv, idx) => vcv.veType.makeCVector(s"o_${idx}") }
        )

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${mergeFn.name}").getCount should be (1L)

        outputs.size should be (2)
        outputs(0).toBytePointerColVector2.toSeqOpt[Double].flatten should be (Seq[Double](1, 2, 3, -1, 2, 3, 4))
        outputs(1).toBytePointerColVector2.toSeqOpt[String].flatten should be (Seq("a", "b", "c", "x", "d", "e", "f"))
      }
    }

    "correctly execute a join function" in {
      // Batch L1
      val c1a = Seq[Double](1, 2, 3, -1).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c1b = Seq("a", "b", "c", "x").map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val lb1 = VeColBatch(Seq(c1a, c1b))

      // Batch L2
      val c2a = Seq[Double](2, 3, 4).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c2b = Seq("d", "e", "f").map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val lb2 = VeColBatch(Seq(c2a, c2b))

      // Batch L3
      val c3a = Seq[Double](5, 6).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c3b = Seq("g", "h").map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val lb3 = VeColBatch(Seq(c3a, c3b))

      // Batch R1
      val c4a = Seq[Int](1, 101).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c4b = Seq("vv", "ww").map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c4c = Seq[Float](3.14f, 3.15f).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val rb1 = VeColBatch(Seq(c4a, c4b, c4c))

      // Batch R2
      val c5a = Seq[Int](2, 103, 104).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c5b = Seq("xx", "yy", "zz").map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c5c = Seq[Float](2.71f, 2.72f, 2.73f).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val rb2 = VeColBatch(Seq(c5a, c5b, c5c))

      // Left batch of batches
      val lbatches = VeBatchOfBatches(Seq(lb1, lb2, lb3))
      // Right batch of batches
      val rbatches = VeBatchOfBatches(Seq(rb1, rb2))

      // Define join function
      val joinFn = SimpleEquiJoinFunction("join_func", lbatches.veTypes , rbatches.veTypes)

      compiledWithHeaders(joinFn.toCFunction) { path =>
        val lib = process.load(path)

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${joinFn.name}") should be (null)

        val outputs = engine.executeJoin(
          lib,
          joinFn.name,
          left = lbatches,
          right = rbatches,
          outputs = joinFn.outputs
        )

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${joinFn.name}").getCount should be (1L)

        // There should be 4 columns - one for the joining column, and three for the remaining
        outputs.size should be (4)
        outputs(0).toBytePointerColVector2.toSeqOpt[Double].flatten should be (Seq[Double](1, 2, 2))
        outputs(1).toBytePointerColVector2.toSeqOpt[String].flatten should be (Seq("a", "b", "d"))
        outputs(2).toBytePointerColVector2.toSeqOpt[String].flatten should be (Seq("vv", "xx", "xx"))
        outputs(3).toBytePointerColVector2.toSeqOpt[Float].flatten should be (Seq[Float](3.14f, 2.71f, 2.71f))
      }
    }

    "correctly execute a grouping function" in {
      // Batch 1
      val c1a = Seq[Long](1, 2, 7, 9).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c1b = Seq[Long](101, 102, 107, 109).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val lb1 = VeColBatch(Seq(c1a, c1b))

      // Batch 2
      val c2a = Seq[Long](10, 11, 15).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c2b = Seq[Long](110, 111, 115).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val lb2 = VeColBatch(Seq(c2a, c2b))

      // Batch 3
      val c3a = Seq[Long](22, 26).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val c3b = Seq[Long](122, 126).map(Some(_)).toBytePointerColVector("_").toVeColVector2
      val lb3 = VeColBatch(Seq(c3a, c3b))

      val batches = VeBatchOfBatches(Seq(lb1, lb2, lb3))

      val groupFn = CppTranspiler.transpileGroupBy(reify { a: (Long, Long) => a._2 % 7 })

      compiledWithHeaders(groupFn.func) { path =>
        val lib = process.load(path)

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${groupFn.func.name}") should be (null)

        val outputs = engine.executeGrouping[Long](
          lib,
          groupFn.func.name,
          inputs = batches,
          outputs = groupFn.outputs
        )

        engine.metrics.getTimers.get(s"${VectorEngine.ExecCallDurationsMetric}.${groupFn.func.name}").getCount should be (1L)

        // Given the example input above, there are no values where a._2 % 7 == 6
        outputs.size should be (6)

        // Unpack to tuples
        val results = outputs.map { case (key, vecs) =>
          (
            vecs(0).toBytePointerColVector2.toSeqOpt[Long].flatten,
            vecs(1).toBytePointerColVector2.toSeqOpt[Long].flatten
          ).zipped.toList
        }

        val expected: Seq[Seq[(Long, Long)]] = Seq(
          Seq((26,126)),
          Seq((7, 107)),
          Seq((1, 101), (15,115), (22, 122)),
          Seq((2, 102), (9, 109)),
          Seq((10, 110)),
          Seq((11, 111))
        )

        results should be (expected)
      }
    }
  }
}