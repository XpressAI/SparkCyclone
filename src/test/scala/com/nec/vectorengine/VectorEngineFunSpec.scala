package com.nec.vectorengine

import com.nec.cache.BpcvTransferDescriptor
import com.nec.colvector.ArrayTConversions._
import com.nec.colvector.SeqOptTConversions._
import com.nec.colvector._
import com.nec.cyclone.annotations.VectorEngineTest
import com.nec.native.transpiler.CppTranspiler
import com.nec.spark.agile.core._
import com.nec.spark.agile.exchange.GroupingFunction
import com.nec.spark.agile.join.SimpleEquiJoinFunction
import com.nec.spark.agile.merge.MergeFunction
import com.nec.util.CallContextOps._
import com.nec.native.compiler.VeKernelInfra
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.reflect.runtime.universe._
import scala.util.Random

@VectorEngineTest
final class VectorEngineFunSpec extends AnyWordSpec with WithVeProcess with VeKernelInfra {
  "VectorEngine" should {
    "correctly execute a basic VE function [1] (single input, single output)" in {
      val func = SampleVeFunctions.DoublingFunction

      withCompiled(func) { path =>
        val lib = process.load(path)

        val inputs = InputSamples.seqOpt[Double](Random.nextInt(20) + 10)
        val colvec = inputs.toBytePointerColVector("_").toVeColVector

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
        outputs.map(_.toBytePointerColVector.toSeqOpt[Double]) should be (Seq(expected))

        // Allocations should have been registered for tracking by VeProcess
        noException should be thrownBy {
          outputs.map(_.free)
        }
      }
    }

    "correctly execute a basic VE function [2] (multi input, multi output)" in {
      val func = SampleVeFunctions.FilterEvensFunction

      withCompiled(func) { path =>
        val lib = process.load(path)

        val inputs0 = InputSamples.array[Int](Random.nextInt(20) + 10)
        val inputs1 = InputSamples.seqOpt[Double](inputs0.size)
        val inputs2 = InputSamples.seqOpt[Double](inputs0.size)

        val colvec0 = inputs0.toBytePointerColVector("_").toVeColVector
        val colvec1 = inputs1.toBytePointerColVector("_").toVeColVector
        val colvec2 = inputs2.toBytePointerColVector("_").toVeColVector

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

        outputs.map(_.toBytePointerColVector.toSeqOpt[Double]) should be (Seq(expected1, expected2))

        noException should be thrownBy {
          outputs.map(_.free)
        }
      }
    }

    "correctly execute a multi-function [1] (simple partitioning)" in {
      val func = SampleVeFunctions.PartitioningFunction

      withCompiled(func) { path =>
        val lib = process.load(path)
        val colvec = Seq[Double](95, 99, 105, 500, 501).map(Some(_)).toBytePointerColVector("_").toVeColVector

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

        noException should be thrownBy {
          outputs.flatMap(_._2).map(_.free)
        }
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

      withCompiled(groupingFn.toCFunction) { path =>
        val lib = process.load(path)

        val lastString = "cccc"
        val colvec1 = Seq[Double](1, 2, 3).map(Some(_)).toBytePointerColVector("_").toVeColVector
        val colvec2 = Seq[Double](9, 8, 7).map(Some(_)).toBytePointerColVector("_").toVeColVector
        val colvecS = Seq("a", "b", lastString).map(Some(_)).toBytePointerColVector("_").toVeColVector

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
            vecs(0).toBytePointerColVector.toSeqOpt[Double].flatten,
            vecs(1).toBytePointerColVector.toSeqOpt[String].flatten,
            vecs(2).toBytePointerColVector.toSeqOpt[Double].flatten,
          ).zipped.toSeq
        }

        results.map(_._2.size).toSet == Set(1, 2)
        results.flatMap(_._2).toSet should be (Set[(Double, String, Double)]((1, "a", 9), (2, "b", 8), (3, lastString, 7)))

        noException should be thrownBy {
          outputs.flatMap(_._2).map(_.free)
        }
      }
    }

    "correctly execute a multi-in function [1] (merge multiple VeColBatches)" in {
      val mergeFn = MergeFunction("merge_func", Seq(VeNullableDouble, VeString))

      withCompiled(mergeFn.toCFunction) { path =>
        val lib = process.load(path)

        val colvec1 = Seq[Double](1, 2, 3, -1).map(Some(_)).toBytePointerColVector("_").toVeColVector
        val colvec2 = Seq[Double](2, 3, 4).map(Some(_)).toBytePointerColVector("_").toVeColVector
        val svec1 = Seq("a", "b", "c", "x").map(Some(_)).toBytePointerColVector("_").toVeColVector
        val svec2 = Seq("d", "e", "f").map(Some(_)).toBytePointerColVector("_").toVeColVector

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
        outputs(0).toBytePointerColVector.toSeqOpt[Double].flatten should be (Seq[Double](1, 2, 3, -1, 2, 3, 4))
        outputs(1).toBytePointerColVector.toSeqOpt[String].flatten should be (Seq("a", "b", "c", "x", "d", "e", "f"))

        noException should be thrownBy {
          outputs.map(_.free)
        }
      }
    }

    "correctly execute a join function" in {
      // Batch L1
      val c1a = Seq[Double](1, 2, 3, -1).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c1b = Seq("a", "b", "c", "x").map(Some(_)).toBytePointerColVector("_").toVeColVector
      val lb1 = VeColBatch(Seq(c1a, c1b))

      // Batch L2
      val c2a = Seq[Double](2, 3, 4).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c2b = Seq("d", "e", "f").map(Some(_)).toBytePointerColVector("_").toVeColVector
      val lb2 = VeColBatch(Seq(c2a, c2b))

      // Batch L3
      val c3a = Seq[Double](5, 6).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c3b = Seq("g", "h").map(Some(_)).toBytePointerColVector("_").toVeColVector
      val lb3 = VeColBatch(Seq(c3a, c3b))

      // Batch R1
      val c4a = Seq[Int](1, 101).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c4b = Seq("vv", "ww").map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c4c = Seq[Float](3.14f, 3.15f).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val rb1 = VeColBatch(Seq(c4a, c4b, c4c))

      // Batch R2
      val c5a = Seq[Int](2, 103, 104).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c5b = Seq("xx", "yy", "zz").map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c5c = Seq[Float](2.71f, 2.72f, 2.73f).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val rb2 = VeColBatch(Seq(c5a, c5b, c5c))

      // Left batch of batches
      val lbatches = VeBatchOfBatches(Seq(lb1, lb2, lb3))
      // Right batch of batches
      val rbatches = VeBatchOfBatches(Seq(rb1, rb2))

      // Define join function
      val joinFn = SimpleEquiJoinFunction("join_func", lbatches.veTypes , rbatches.veTypes)

      withCompiled(joinFn.toCFunction) { path =>
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
        outputs(0).toBytePointerColVector.toSeqOpt[Double].flatten should be (Seq[Double](1, 2, 2))
        outputs(1).toBytePointerColVector.toSeqOpt[String].flatten should be (Seq("a", "b", "d"))
        outputs(2).toBytePointerColVector.toSeqOpt[String].flatten should be (Seq("vv", "xx", "xx"))
        outputs(3).toBytePointerColVector.toSeqOpt[Float].flatten should be (Seq[Float](3.14f, 2.71f, 2.71f))

        noException should be thrownBy {
          outputs.map(_.free)
        }
      }
    }

    "correctly execute a grouping function" in {
      // Batch 1
      val c1a = Seq[Long](1, 2, 7, 9).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c1b = Seq[Long](101, 102, 107, 109).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val lb1 = VeColBatch(Seq(c1a, c1b))

      // Batch 2
      val c2a = Seq[Long](10, 11, 15).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c2b = Seq[Long](110, 111, 115).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val lb2 = VeColBatch(Seq(c2a, c2b))

      // Batch 3
      val c3a = Seq[Long](22, 26).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val c3b = Seq[Long](122, 126).map(Some(_)).toBytePointerColVector("_").toVeColVector
      val lb3 = VeColBatch(Seq(c3a, c3b))

      val batches = VeBatchOfBatches(Seq(lb1, lb2, lb3))

      val groupFn = CppTranspiler.transpileGroupBy(reify { a: (Long, Long) => a._2 % 7 })

      withCompiled(groupFn.func) { path =>
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
            vecs(0).toBytePointerColVector.toSeqOpt[Long].flatten,
            vecs(1).toBytePointerColVector.toSeqOpt[Long].flatten
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

        noException should be thrownBy {
          outputs.flatMap(_._2).map(_.free)
        }
      }
    }

    s"correctly execute bulk data transfer to the VE using ${classOf[BpcvTransferDescriptor].getSimpleName}" in {
      // Batch A
      val sizeA = Random.nextInt(100) + 10
      val a1 = InputSamples.seqOpt[Int](sizeA)
      val a2 = InputSamples.seqOpt[Double](sizeA)
      val a3 = InputSamples.seqOpt[String](sizeA)
      // println(s"a1 = ${a1}")
      // println(s"a2 = ${a2}")
      // println(s"a3 = ${a3}")

      // Batch B
      val sizeB = Random.nextInt(100) + 10
      val b1 = InputSamples.seqOpt[Int](sizeB)
      val b2 = InputSamples.seqOpt[Double](sizeB)
      val b3 = InputSamples.seqOpt[String](sizeB)
      // println(s"b1 = ${b1}")
      // println(s"b2 = ${b2}")
      // println(s"b3 = ${b3}")

      // Batch C
      val sizeC = Random.nextInt(100) + 10
      val c1 = InputSamples.seqOpt[Int](sizeC)
      val c2 = InputSamples.seqOpt[Double](sizeC)
      val c3 = InputSamples.seqOpt[String](sizeC)
      // println(s"c1 = ${c1}")
      // println(s"c2 = ${c2}")
      // println(s"c3 = ${c3}")

      // Create batch of batches
      val descriptor = BpcvTransferDescriptor(Seq(
        Seq(a1.toBytePointerColVector("_"), a2.toBytePointerColVector("_"), a3.toBytePointerColVector("_")),
        Seq(b1.toBytePointerColVector("_"), b2.toBytePointerColVector("_"), b3.toBytePointerColVector("_")),
        Seq(c1.toBytePointerColVector("_"), c2.toBytePointerColVector("_"), c3.toBytePointerColVector("_"))
      ))

      // Create the VeColBatch via bulk data transfer
      val batch = engine.executeTransfer(descriptor)

      // There should be only 3 columns
      batch.columns.size should be (3)

      // Transfer back to VH - the Nth column of each batch should be consolidated
      batch.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (a1 ++ b1 ++ c1)
      batch.columns(1).toBytePointerColVector.toSeqOpt[Double] should be (a2 ++ b2 ++ c2)
      batch.columns(2).toBytePointerColVector.toSeqOpt[String] should be (a3 ++ b3 ++ c3)

      // The memory created from the VE side should be registered for safe free()
      noException should be thrownBy {
        batch.free
      }
    }

    "correctly transfer a batch of Seq[Option[Int]] to the VE and back without loss of data fidelity [1]" in {
      // Batch A
      val a1 = Seq(None, Some(4436), None, None, Some(9586), Some(2142))

      // Batch B
      val b1 = Seq(None, None, None, Some(8051))

      // Batch C
      val c1 = Seq(Some(7319), None, None, Some(4859), Some(524))

      // Create batch of batches
      val descriptor = BpcvTransferDescriptor(Seq(
        Seq(a1.toBytePointerColVector("_")),
        Seq(b1.toBytePointerColVector("_")),
        Seq(c1.toBytePointerColVector("_"))
      ))

      // Create the VeColBatch via bulk data transfer
      val batch = engine.executeTransfer(descriptor)

      // There should be only 1 columns
      batch.columns.size should be (1)

      // Transfer back to VH - the Nth column of each batch should be consolidated
      batch.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (a1 ++ b1 ++ c1)

      // The memory created from the VE side should be registered for safe free()
      noException should be thrownBy {
        batch.free
      }
    }

    "correctly transfer a batch of Seq[Option[Int]] to the VE and back without loss of data fidelity [2]" in {
      // Batch A
      val a1 = Seq(None, Some(4436), None, None, Some(9586), Some(2142), None, None, None, Some(2149), Some(4297), None, None, Some(3278), Some(6668), None)

      // Batch B
      val b1 = Seq(None, None, None, Some(8051), None, Some(1383), None, None, Some(2256), Some(5785), None, None, None, None, None, Some(4693), None, Some(1849), Some(3790), Some(8995), None, Some(6961), Some(7132), None, None, None, None, Some(6968), None, None, Some(3763), None, Some(3558), None, None, Some(2011), None, None, None, Some(3273), None, None, Some(9428), None, None, Some(6408), Some(7940), None, Some(9521), None, None, Some(5832), None, None, Some(5817), Some(5949))

      // Batch C
      val c1 = Seq(Some(7319), None, None, Some(4859), Some(524), Some(406), None, None, Some(1154), None, None, Some(1650), Some(8040), None, None, None, None, None, None, None, None, None, Some(1146), None, Some(7268), Some(8197), None, None, None, None, Some(81), Some(2053), Some(6571), Some(4600), None, Some(3699), None, Some(8404), None, None, Some(8401), None, None, Some(6234), Some(6281), Some(7367), None, Some(4688), Some(7490), None, Some(5412), None, None, Some(871), None, Some(9086), None, Some(5362), Some(6516))

      // Create batch of batches
      val descriptor = BpcvTransferDescriptor(Seq(
        Seq(a1.toBytePointerColVector("_")),
        Seq(b1.toBytePointerColVector("_")),
        Seq(c1.toBytePointerColVector("_"))
      ))

      // Create the VeColBatch via bulk data transfer
      val batch = engine.executeTransfer(descriptor)

      // There should be only 1 columns
      batch.columns.size should be (1)

      // Transfer back to VH - the Nth column of each batch should be consolidated
      batch.columns(0).toBytePointerColVector.toSeqOpt[Int] should be (a1 ++ b1 ++ c1)

      // The memory created from the VE side should be registered for safe free()
      noException should be thrownBy {
        batch.free
      }
    }
  }
}
