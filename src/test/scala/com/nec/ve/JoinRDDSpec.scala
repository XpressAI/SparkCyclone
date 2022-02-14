package com.nec.ve

import com.nec.arrow.ArrowVectorBuilders.withDirectFloat8Vector
import com.nec.arrow.WithTestAllocator
import com.nec.spark.{SparkAdditions, SparkCycloneExecutorPlugin}
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.DetectVectorEngineSpec.VeClusterConfig
import com.nec.ve.JoinRDDSpec.testJoin
import com.nec.ve.VeColBatch.{VeColVector, VeColVectorSource}
import com.nec.ve.VeProcess.OriginalCallingContext
import org.apache.arrow.vector.Float8Vector
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec

final class JoinRDDSpec extends AnyFreeSpec with SparkAdditions with VeKernelInfra {

  "Join data across partitioned data" in {
    val result =
      withSparkSession2(
        VeClusterConfig
          .andThen(DynamicVeSqlExpressionEvaluationSpec.VeConfiguration)
      ) { sparkSession =>
        testJoin(sparkSession)
      }.sortBy(_._1.head)

    val expected: List[(List[Double], List[Double])] =
      List(
        List[Double](3, 4, 5) -> List[Double](5, 6, 7),
        List[Double](5, 6, 7) -> List[Double](8, 8, 7)
      )

    assert(result == expected)
  }

}

object JoinRDDSpec {

  def testJoin(sparkSession: SparkSession): List[(List[Double], List[Double])] = {

    val partsL: List[(Int, List[Double])] =
      List(1 -> List(3, 4, 5), 2 -> List(5, 6, 7))
    val partsR: List[(Int, List[Double])] =
      List(1 -> List(5, 6, 7), 2 -> List(8, 8, 7), 3 -> List(9, 6, 7))
    import SparkCycloneExecutorPlugin._
    VeRDD
      .joinExchangeLB(
        sparkSession.sparkContext.makeRDD(partsL).map { case (i, l) =>
          import OriginalCallingContext.Automatic._
          i -> VeColBatch.fromList(List(l.toVeColVector()))
        },
        sparkSession.sparkContext.makeRDD(partsR).map { case (i, l) =>
          import OriginalCallingContext.Automatic._
          i -> VeColBatch.fromList(List(l.toVeColVector()))
        },
        cleanUpInput = true
      )
      .map { case (la, lb) =>
        (la.cols.flatMap(_.toList), lb.cols.flatMap(_.toList))
      }
      .collect()
      .toList
  }
  implicit class RichDoubleList(l: List[Double]) {
    def toVeColVector()(implicit
      veProcess: VeProcess,
      source: VeColVectorSource,
      originalCallingContext: OriginalCallingContext
    ): VeColVector = {
      WithTestAllocator { implicit alloc =>
        withDirectFloat8Vector(l) { vec =>
          VeColVector.fromArrowVector(vec)
        }
      }
    }
  }

  trait ColVectorDecoder[T] {
    def decode(
      veColVector: VeColVector
    )(implicit veProcess: VeProcess, source: VeColVectorSource): List[T]
  }
  object ColVectorDecoder {
    implicit val decodeDouble: ColVectorDecoder[Double] = new ColVectorDecoder[Double] {
      override def decode(
        veColVector: VeColVector
      )(implicit veProcess: VeProcess, source: VeColVectorSource): List[Double] = {
        WithTestAllocator { implicit alloc =>
          veColVector.toArrowVector().asInstanceOf[Float8Vector].toList
        }
      }
    }
  }
  implicit class RichVeColVector(veColVector: VeColVector) {
    def toList[T: ColVectorDecoder](implicit
      veProcess: VeProcess,
      source: VeColVectorSource
    ): List[T] =
      implicitly[ColVectorDecoder[T]].decode(veColVector)
  }

  val MultiFunctionName = "f_multi"

}
