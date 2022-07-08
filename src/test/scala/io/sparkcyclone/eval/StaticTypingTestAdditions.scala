/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.sparkcyclone.eval

import io.sparkcyclone.data.conversion.SeqOptTConversions._
import io.sparkcyclone.data.conversion.ArrowVectorConversions._
import io.sparkcyclone.spark.codegen.core._
import io.sparkcyclone.spark.codegen.join.JoinUtils._
import io.sparkcyclone.util.CallContext
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.VeColVectorSource
import io.sparkcyclone.data.vector.VeColBatch
import io.sparkcyclone.metrics.VeProcessMetrics
import io.sparkcyclone.vectorengine.VeProcess

/**
 * Boilerplate to deal with making the tests nice and tight.
 *
 * This can be made generic with shapeless, however for our use case we should just
 * push all the dirty boilerplate to here, away from the test cases, so that the team
 * can focus on doing testing rather than writing boilerplate.
 *
 * There still is further to room to make them cleaner.
 */
object StaticTypingTestAdditions {

  implicit val metrics = VeProcessMetrics.noOp
  trait VeAllocator[Input] {
    def allocate(data: Input*)(implicit
      veProcess: VeProcess,
      context: CallContext,
      veColVectorSource: VeColVectorSource
    ): VeColBatch
    def veTypes: List[VeType]
    final def makeCVectors: List[CVector] = veTypes.zipWithIndex.map { case (veType, idx) =>
      veType.makeCVector(s"input_${idx}")
    }
  }

  object VeAllocator {
    implicit object DoubleAllocator extends VeAllocator[Double] {
      override def allocate(data: Double*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(data.map(Some(_)).toBytePointerColVector("_").toVeColVector)
      }

      override def veTypes: List[VeType] = List(VeNullableDouble)
    }

    implicit object StringAllocator extends VeAllocator[String] {
      override def allocate(data: String*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(data.map(Some(_)).toBytePointerColVector("_").toVeColVector)
      }

      override def veTypes: List[VeType] = List(VeString)
    }

    implicit object StringDoubleAllocator extends VeAllocator[(String, Double)] {
      override def allocate(data: (String, Double)*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(
          data.map(x => Some(x._1)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._2)).toBytePointerColVector("_").toVeColVector
        )
      }

      override def veTypes: List[VeType] = List(VeNullableDouble)
    }

    implicit object DoubleDoubleAllocator extends VeAllocator[(Double, Double)] {
      override def allocate(data: (Double, Double)*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(
          data.map(x => Some(x._1)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._2)).toBytePointerColVector("_").toVeColVector
        )
      }

      override def veTypes: List[VeType] = List(VeNullableDouble, VeNullableDouble)
    }

    implicit object DoubleDoubleDoubleAllocator extends VeAllocator[(Double, Double, Double)] {
      override def allocate(data: (Double, Double, Double)*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(
          data.map(x => Some(x._1)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._2)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._3)).toBytePointerColVector("_").toVeColVector
        )
      }

      override def veTypes: List[VeType] =
        List(VeNullableDouble, VeNullableDouble, VeNullableDouble)
    }

    implicit object DoubleDoubleDoubleDoubleAllocator
      extends VeAllocator[(Double, Double, Double, Double)] {
      override def allocate(data: (Double, Double, Double, Double)*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(
          data.map(x => Some(x._1)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._2)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._3)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._4)).toBytePointerColVector("_").toVeColVector
        )
      }

      override def veTypes: List[VeType] =
        List(VeNullableDouble, VeNullableDouble, VeNullableDouble, VeNullableDouble)
    }

    implicit object OptionDoubleAllocator extends VeAllocator[Option[Double]] {
      override def allocate(data: Option[Double]*)(implicit process: VeProcess,
                                                   context: CallContext,
                                                   source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(data.toBytePointerColVector("_").toVeColVector)
      }

      override def veTypes: List[VeType] = List(VeNullableDouble)
    }

    implicit object OptionDoubleDoubleDoubleAllocator
      extends VeAllocator[(Option[Double], Double, Double)] {
      override def allocate(data: (Option[Double], Double, Double)*)(implicit process: VeProcess,
                                           context: CallContext,
                                           source: VeColVectorSource): VeColBatch = {
        VeColBatch.from(
          data.map(x => x._1).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._2)).toBytePointerColVector("_").toVeColVector,
          data.map(x => Some(x._3)).toBytePointerColVector("_").toVeColVector,
        )
      }

      override def veTypes: List[VeType] =
        List(VeNullableDouble, VeNullableDouble, VeNullableDouble)
    }

  }
  trait VeRetriever[Output] {
    final def makeCVectors: List[CVector] = veTypes.zipWithIndex.map {
      case (veType, idx) => veType.makeCVector(s"output_${idx}")
    }
    def retrieve(veColBatch: VeColBatch)(implicit veProcess: VeProcess): Seq[Output]
    def veTypes: List[VeType]
  }
  object VeRetriever {
    implicit object DoubleDoubleRetriever extends VeRetriever[(Double, Double)] {
      override def veTypes: List[VeType] = List(VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(Double, Double)] = {
        veColBatch.columns.map { col =>
          col.toBytePointerColVector.toSeqOpt[Double].flatten.toList
        } match {
          case colA :: colB :: Nil => colA.zip(colB)
        }
      }
    }
    implicit object DoubleDoubleDoubleRetriever extends VeRetriever[(Double, Double, Double)] {
      override def veTypes: List[VeType] =
        List(VeNullableDouble, VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(Double, Double, Double)] = {
        veColBatch.columns.map { col =>
          col.toBytePointerColVector.toSeqOpt[Double].flatten.toList

        } match {
          case colA :: colB :: colC :: Nil =>
            colA.zip(colB).zip(colC).map { case ((a, b), c) =>
              (a, b, c)
            }
        }
      }
    }
    implicit object DoubleDoubleDoubleDoubleRetriever
      extends VeRetriever[(Double, Double, Double, Double)] {
      override def veTypes: List[VeType] =
        List(VeNullableDouble, VeNullableDouble, VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(Double, Double, Double, Double)] = {
        veColBatch.columns.map { col =>
          col.toBytePointerColVector.toSeqOpt[Double].flatten.toList
        } match {
          case colA :: colB :: colC :: colD :: Nil =>
            colA.zip(colB).zip(colC).zip(colD).map { case (((a, b), c), d) =>
              (a, b, c, d)
            }
        }
      }
    }
    implicit object OptionDoubleRetriever extends VeRetriever[Option[Double]] {
      override def veTypes: List[VeType] = List(VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): Seq[Option[Double]] = {
        veColBatch.columns.flatMap { col =>
          col.toBytePointerColVector.toSeqOpt[Double].toList
        }
      }
    }
    implicit object IntRetriever extends VeRetriever[Int] {
      override def veTypes: List[VeType] = List(VeNullableInt)

      override def retrieve(veColBatch: VeColBatch)(implicit veProcess: VeProcess): Seq[Int] = {
        veColBatch.columns.flatMap { col =>
          col.toBytePointerColVector.toSeqOpt[Int].flatten.toList
        }
      }
    }
    implicit object DoubleRetriever extends VeRetriever[Double] {
      override def veTypes: List[VeType] = List(VeNullableDouble)

      override def retrieve(veColBatch: VeColBatch)(implicit veProcess: VeProcess): Seq[Double] = {
        veColBatch.columns.flatMap { col =>
          col.toBytePointerColVector.toSeqOpt[Double].flatten.toList
        }
      }
    }

    implicit object DoubleOptionDoubleRetriever extends VeRetriever[(Double, Option[Double])] {
      override def veTypes: List[VeType] = List(VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(Double, Option[Double])] = {
        veColBatch.columns.map { col =>
          col.toBytePointerColVector.toSeqOpt[Double].toList
        } match {
          case colA :: colB :: Nil => colA.flatten.zip(colB)
        }
      }
    }
    implicit object OptionDoubleDoubleDoubleRetriever
      extends VeRetriever[(Option[Double], Double, Double)] {
      override def veTypes: List[VeType] = List(VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(Option[Double], Double, Double)] = {
        veColBatch.columns.map { col =>
          col.toBytePointerColVector.toSeqOpt[Double].toList
        } match {
          case colA :: colB :: colC :: Nil =>
            colA.zip(colB.flatten).zip(colC.flatten).map { case ((a, b), c) => (a, b, c) }
        }
      }
    }
    implicit object OptionDoubleDoubleOptionDoubleRetriever
      extends VeRetriever[(Option[Double], Double, Option[Double])] {
      override def veTypes: List[VeType] = List(VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(Option[Double], Double, Option[Double])] = {
        veColBatch.columns.map { col =>
          col.toBytePointerColVector.toSeqOpt[Double].toList
        } match {
          case colA :: colB :: colC :: Nil =>
            colA.zip(colB.flatten).zip(colC).map { case ((a, b), c) => (a, b, c) }
        }
      }
    }
    implicit object StringDoubleRetriever extends VeRetriever[(String, Double)] {
      override def veTypes: List[VeType] = List(VeNullableDouble, VeNullableDouble)

      override def retrieve(
        veColBatch: VeColBatch
      )(implicit veProcess: VeProcess): List[(String, Double)] = {
        val colA :: colB :: Nil = veColBatch.columns.map(_.toBytePointerColVector)
        colA.toSeqOpt[String].flatten.toList.zip(colB.toSeqOpt[Double].flatten.toList)
      }
    }
  }

  trait JoinExpressor[Output] {
    def express(output: Output): List[NamedJoinExpression]
  }

  object JoinExpressor {
    implicit class RichJoin[T](t: T)(implicit joinExpressor: JoinExpressor[T]) {
      def expressed: List[NamedJoinExpression] = joinExpressor.express(t)
    }
    implicit val forQuartetDouble: JoinExpressor[
      (
        TypedJoinExpression[Double],
        TypedJoinExpression[Double],
        TypedJoinExpression[Double],
        TypedJoinExpression[Double]
      )
    ] = output =>
      List(
        NamedJoinExpression("output_1", VeNullableDouble, output._1.joinExpression),
        NamedJoinExpression("output_2", VeNullableDouble, output._2.joinExpression),
        NamedJoinExpression("output_3", VeNullableDouble, output._3.joinExpression),
        NamedJoinExpression("output_4", VeNullableDouble, output._4.joinExpression)
      )

    implicit val forQuartetDoubleOption: JoinExpressor[
      (
        TypedJoinExpression[Option[Double]],
        TypedJoinExpression[Option[Double]],
        TypedJoinExpression[Option[Double]],
        TypedJoinExpression[Option[Double]]
      )
    ] = output =>
      List(
        NamedJoinExpression("output_1", VeNullableDouble, output._1.joinExpression),
        NamedJoinExpression("output_2", VeNullableDouble, output._2.joinExpression),
        NamedJoinExpression("output_3", VeNullableDouble, output._3.joinExpression),
        NamedJoinExpression("output_4", VeNullableDouble, output._4.joinExpression)
      )
  }
}
