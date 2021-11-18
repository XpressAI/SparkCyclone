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
package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.{
  VectorInputNativeArgument,
  VectorOutputNativeArgument
}
import com.nec.util.RichVectors._
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.StringProducer
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, VarCharVector}

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

  trait InputArgumentsFull[Input] {
    def allocateVectors(data: Input*)(implicit
      rootAllocator: RootAllocator
    ): List[VectorInputNativeArgument]
    def inputs: List[CVector]
  }

  object InputArgumentsFull {
    implicit def fromScalar[I](implicit
      inputArgumentsScalar: InputArgumentsScalar[I]
    ): InputArgumentsFull[I] = new InputArgumentsFull[I] {
      override def allocateVectors(data: I*)(implicit
        rootAllocator: RootAllocator
      ): List[VectorInputNativeArgument] = inputArgumentsScalar.allocateVectors(data: _*)

      override def inputs: List[CVector] = inputArgumentsScalar.inputs
    }

    implicit val forString: InputArgumentsFull[String] = new InputArgumentsFull[String] {
      override def allocateVectors(
        data: String*
      )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
        inputs.zipWithIndex.map { case (cv, idx) =>
          val vcv = new VarCharVector(cv.name, rootAllocator)
          vcv.allocateNew()
          vcv.setValueCount(data.size)
          data.zipWithIndex.foreach { case (str, idx) =>
            vcv.setSafe(idx, str.getBytes())
          }
          NativeArgument.input(vcv)
        }
      }

      override def inputs: List[CVector] = List(CVarChar("input_0"))
    }
    implicit val forStringDouble: InputArgumentsFull[(String, Double)] =
      new InputArgumentsFull[(String, Double)] {
        override def allocateVectors(
          data: (String, Double)*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (cv, idx) =>
            if (idx == 0) {
              val vcv = new VarCharVector(cv.name, rootAllocator)
              vcv.allocateNew()
              vcv.setValueCount(data.size)
              data.zipWithIndex.foreach { case ((str, dbl), idx) =>
                vcv.setSafe(idx, str.getBytes())
              }
              NativeArgument.input(vcv)
            } else {
              val f8v = new Float8Vector(cv.name, rootAllocator)
              f8v.allocateNew()
              f8v.setValueCount(data.size)
              data.zipWithIndex.foreach { case ((str, dbl), idx) =>
                f8v.setSafe(idx, dbl)
              }
              NativeArgument.input(f8v)
            }
          }
        }

        override def inputs: List[CVector] =
          List(CVarChar("input_0"), CScalarVector("input_1", VeScalarType.veNullableDouble))
      }
  }

  trait InputArgumentsScalar[Input] {
    def allocateVectors(data: Input*)(implicit
      rootAllocator: RootAllocator
    ): List[VectorInputNativeArgument]
    def inputs: List[CScalarVector]
  }
  object InputArgumentsScalar {

    implicit val forDouble: InputArgumentsScalar[Double] = new InputArgumentsScalar[Double] {
      override def allocateVectors(
        data: Double*
      )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
        inputs.zipWithIndex.map { case (CScalarVector(name, tpe), idx) =>
          val vcv = new Float8Vector(name, rootAllocator)
          vcv.allocateNew()
          vcv.setValueCount(data.size)
          data.zipWithIndex.foreach { case (str, idx) =>
            vcv.setSafe(idx, str)
          }
          NativeArgument.input(vcv)
        }
      }

      override def inputs: List[CScalarVector] = List(
        CScalarVector("input_0", VeScalarType.veNullableDouble)
      )
    }
    implicit val forDoubleOpt: InputArgumentsScalar[Option[Double]] =
      new InputArgumentsScalar[Option[Double]] {
        override def allocateVectors(
          data: Option[Double]*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (CScalarVector(name, tpe), idx) =>
            val vcv = new Float8Vector(name, rootAllocator)
            vcv.allocateNew()
            vcv.setValueCount(data.size)
            data.zipWithIndex.foreach { case (str, idx) =>
              str match {
                case None    => vcv.setNull(idx)
                case Some(v) => vcv.setSafe(idx, v)
              }
            }
            NativeArgument.input(vcv)
          }
        }

        override def inputs: List[CScalarVector] = List(
          CScalarVector("input_0", VeScalarType.veNullableDouble)
        )
      }
    implicit val forPairDouble: InputArgumentsScalar[(Double, Double)] =
      new InputArgumentsScalar[(Double, Double)] {
        override def allocateVectors(
          data: (Double, Double)*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (CScalarVector(name, tpe), idx_col) =>
            val vcv = new Float8Vector(name, rootAllocator)
            vcv.allocateNew()
            vcv.setValueCount(data.size)
            data.zipWithIndex.foreach { case ((first, second), idx) =>
              vcv.setSafe(idx, if (idx_col == 0) first else second)
            }
            NativeArgument.input(vcv)
          }
        }

        override def inputs: List[CScalarVector] = List(
          CScalarVector("input_0", VeScalarType.veNullableDouble),
          CScalarVector("input_1", VeScalarType.veNullableDouble)
        )
      }
    implicit val forTrupleDouble: InputArgumentsScalar[(Double, Double, Double)] =
      new InputArgumentsScalar[(Double, Double, Double)] {
        override def allocateVectors(
          data: (Double, Double, Double)*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (CScalarVector(name, tpe), idx_col) =>
            val vcv = new Float8Vector(name, rootAllocator)
            vcv.allocateNew()
            vcv.setValueCount(data.size)
            data.zipWithIndex.foreach { case ((first, second, third), idx) =>
              vcv.setSafe(idx, if (idx_col == 0) first else if (idx_col == 1) second else third)
            }
            NativeArgument.input(vcv)
          }
        }

        override def inputs: List[CScalarVector] = List(
          CScalarVector("input_0", VeScalarType.veNullableDouble),
          CScalarVector("input_1", VeScalarType.veNullableDouble),
          CScalarVector("input_2", VeScalarType.veNullableDouble)
        )
      }
    implicit val forTrupleDouble1Opt: InputArgumentsScalar[(Option[Double], Double, Double)] =
      new InputArgumentsScalar[(Option[Double], Double, Double)] {
        override def allocateVectors(
          data: (Option[Double], Double, Double)*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (CScalarVector(name, tpe), idx_col) =>
            val vcv = new Float8Vector(name, rootAllocator)
            vcv.allocateNew()
            vcv.setValueCount(data.size)
            data.zipWithIndex.foreach { case ((first, second, third), idx) =>
              if (idx_col == 0 && first.isEmpty)
                vcv.setNull(idx)
              else
                vcv.setSafe(
                  idx,
                  if (idx_col == 0) first.get else if (idx_col == 1) second else third
                )
            }
            NativeArgument.input(vcv)
          }
        }

        override def inputs: List[CScalarVector] = List(
          CScalarVector("input_0", VeScalarType.veNullableDouble),
          CScalarVector("input_1", VeScalarType.veNullableDouble),
          CScalarVector("input_2", VeScalarType.veNullableDouble)
        )
      }

    implicit val forQuartetDouble: InputArgumentsScalar[(Double, Double, Double, Double)] =
      new InputArgumentsScalar[(Double, Double, Double, Double)] {
        override def allocateVectors(
          data: (Double, Double, Double, Double)*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (CScalarVector(name, tpe), idx_col) =>
            val vcv = new Float8Vector(name, rootAllocator)
            vcv.allocateNew()
            vcv.setValueCount(data.size)
            data.zipWithIndex.foreach { case ((first, second, third, fourth), idx) =>
              vcv.setSafe(
                idx,
                if (idx_col == 0) first
                else if (idx_col == 1) second
                else if (idx_col == 2) third
                else fourth
              )
            }
            NativeArgument.input(vcv)
          }
        }

        override def inputs: List[CScalarVector] = List(
          CScalarVector("input_0", VeScalarType.veNullableDouble),
          CScalarVector("input_1", VeScalarType.veNullableDouble),
          CScalarVector("input_2", VeScalarType.veNullableDouble),
          CScalarVector("input_3", VeScalarType.veNullableDouble)
        )
      }
  }

  trait OutputArguments[Output] {
    def allocateVectors()(implicit
      rootAllocator: RootAllocator
    ): (List[VectorOutputNativeArgument], () => List[Result])
    type Result
  }

  object OutputArguments {
    implicit val forDouble: OutputArguments[Double] =
      new OutputArguments[Double] {
        override type Result = Double
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          (List(NativeArgument.output(outVector_0)), () => outVector_0.toList)
        }
      }
    implicit val forPairStringDoubleTypedG
      : OutputArguments[(StringGrouping, TypedGroupByExpression[Double])] =
      new OutputArguments[(StringGrouping, TypedGroupByExpression[Double])] {
        override type Result = (String, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new VarCharVector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)

          (
            List(NativeArgument.output(outVector_0), NativeArgument.output(outVector_1)),
            () => {
              outVector_0.toList.zip(outVector_1.toList)
            }
          )
        }
      }
    implicit val forPairStringDoubleTyped
      : OutputArguments[(StringProducer, TypedCExpression[Double])] =
      new OutputArguments[(StringProducer, TypedCExpression[Double])] {
        override type Result = (String, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new VarCharVector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)

          (
            List(NativeArgument.output(outVector_0), NativeArgument.output(outVector_1)),
            () => {
              outVector_0.toList.zip(outVector_1.toList)
            }
          )
        }
      }
    implicit val forPairStringDouble: OutputArguments[(String, Double)] =
      new OutputArguments[(String, Double)] {
        override type Result = (String, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new VarCharVector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)

          (
            List(NativeArgument.output(outVector_0), NativeArgument.output(outVector_1)),
            () => {
              outVector_0.toList.zip(outVector_1.toList)
            }
          )
        }
      }
    implicit val forPairDoubleTyped
      : OutputArguments[(TypedCExpression[Double], TypedCExpression[Double])] =
      new OutputArguments[(TypedCExpression[Double], TypedCExpression[Double])] {
        override type Result = (Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          (
            List(NativeArgument.output(outVector_0), NativeArgument.output(outVector_1)),
            () => outVector_0.toList.zip(outVector_1.toList)
          )
        }
      }
    implicit val forDoubleTyped: OutputArguments[TypedCExpression[Double]] =
      new OutputArguments[TypedCExpression[Double]] {
        override type Result = Double
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          (List(NativeArgument.output(outVector_0)), () => outVector_0.toList)
        }
      }
    implicit val forPairDouble: OutputArguments[(Double, Double)] =
      new OutputArguments[(Double, Double)] {
        override type Result = (Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          (
            List(NativeArgument.output(outVector_0), NativeArgument.output(outVector_1)),
            () => outVector_0.toList.zip(outVector_1.toList)
          )
        }
      }
    implicit val forTripletDouble: OutputArguments[(Double, Double, Double)] =
      new OutputArguments[(Double, Double, Double)] {
        override type Result = (Double, Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)

          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2)
            ),
            () =>
              outVector_0.toList.zip(outVector_1.toList).zip(outVector_2.toList).map {
                case ((a, b), c) => (a, b, c)
              }
          )
        }
      }

    implicit val forPairDoubleOneOption
      : OutputArguments[(TypedCExpression[Double], TypedCExpression[Option[Double]])] =
      new OutputArguments[(TypedCExpression[Double], TypedCExpression[Option[Double]])] {
        override type Result = (Double, Option[Double])
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)

          (
            List(NativeArgument.output(outVector_0), NativeArgument.output(outVector_1)),
            () => outVector_0.toList.zip(outVector_1.toListSafe)
          )
        }
      }
    implicit val forTripletDoubleT: OutputArguments[
      (TypedCExpression[Double], TypedCExpression[Double], TypedCExpression[Double])
    ] =
      new OutputArguments[
        (TypedCExpression[Double], TypedCExpression[Double], TypedCExpression[Double])
      ] {
        override type Result = (Double, Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)
          val outVector_3 = new Float8Vector("output_3", rootAllocator)

          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2)
            ),
            () =>
              outVector_0.toList.zip(outVector_1.toList).zip(outVector_2.toList).map {
                case ((a, b), c) => (a, b, c)
              }
          )
        }
      }
    implicit val forQuartedOptionDoubleJoin: OutputArguments[
      (
        TypedJoinExpression[Option[Double]],
        TypedJoinExpression[Option[Double]],
        TypedJoinExpression[Option[Double]],
        TypedJoinExpression[Option[Double]]
      )
    ] =
      new OutputArguments[
        (
          TypedJoinExpression[Option[Double]],
          TypedJoinExpression[Option[Double]],
          TypedJoinExpression[Option[Double]],
          TypedJoinExpression[Option[Double]]
        )
      ] {
        override type Result = (Option[Double], Option[Double], Option[Double], Option[Double])
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)
          val outVector_3 = new Float8Vector("output_3", rootAllocator)
          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2),
              NativeArgument.output(outVector_3)
            ),
            () =>
              outVector_0.toListNullable
                .zip(outVector_1.toListNullable)
                .zip(outVector_2.toListNullable)
                .zip(outVector_3.toListNullable)
                .map { case (((a, b), c), d) =>
                  (a, b, c, d)
                }
          )
        }
      }

    implicit val forOptionDouble: OutputArguments[Option[Double]] =
      new OutputArguments[Option[Double]] {
        override type Result = (Option[Double])
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          (List(NativeArgument.output(outVector_0)), () => outVector_0.toListNullable)
        }
      }

    implicit val forQuartedDoubleJoin: OutputArguments[
      (
        TypedJoinExpression[Double],
        TypedJoinExpression[Double],
        TypedJoinExpression[Double],
        TypedJoinExpression[Double]
      )
    ] =
      new OutputArguments[
        (
          TypedJoinExpression[Double],
          TypedJoinExpression[Double],
          TypedJoinExpression[Double],
          TypedJoinExpression[Double]
        )
      ] {
        override type Result = (Double, Double, Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)
          val outVector_3 = new Float8Vector("output_3", rootAllocator)
          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2),
              NativeArgument.output(outVector_3)
            ),
            () =>
              outVector_0.toList
                .zip(outVector_1.toList)
                .zip(outVector_2.toList)
                .zip(outVector_3.toList)
                .map { case (((a, b), c), d) =>
                  (a, b, c, d)
                }
          )
        }
      }

    implicit val forTripletDoubleGB: OutputArguments[
      (
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Double]
      )
    ] =
      new OutputArguments[
        (
          TypedGroupByExpression[Double],
          TypedGroupByExpression[Double],
          TypedGroupByExpression[Double]
        )
      ] {
        override type Result = (Double, Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)

          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2)
            ),
            () =>
              outVector_0.toList.zip(outVector_1.toList).zip(outVector_2.toList).map {
                case ((a, b), c) => (a, b, c)
              }
          )
        }
      }
    implicit val forTripletDoubleGBOption: OutputArguments[
      (
        TypedGroupByExpression[Option[Double]],
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Double]
      )
    ] =
      new OutputArguments[
        (
          TypedGroupByExpression[Option[Double]],
          TypedGroupByExpression[Double],
          TypedGroupByExpression[Double]
        )
      ] {
        override type Result = (Option[Double], Double, Double)
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)

          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2)
            ),
            () =>
              outVector_0.toListSafe.zip(outVector_1.toList).zip(outVector_2.toList).map {
                case ((a, b), c) => (a, b, c)
              }
          )
        }
      }
    implicit val forTripletDoubleGBOption2: OutputArguments[
      (
        TypedGroupByExpression[Option[Double]],
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Option[Double]]
      )
    ] =
      new OutputArguments[
        (
          TypedGroupByExpression[Option[Double]],
          TypedGroupByExpression[Double],
          TypedGroupByExpression[Option[Double]]
        )
      ] {
        override type Result = (Option[Double], Double, Option[Double])
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)
          val outVector_1 = new Float8Vector("output_1", rootAllocator)
          val outVector_2 = new Float8Vector("output_2", rootAllocator)

          (
            List(
              NativeArgument.output(outVector_0),
              NativeArgument.output(outVector_1),
              NativeArgument.output(outVector_2)
            ),
            () =>
              outVector_0.toListSafe.zip(outVector_1.toList).zip(outVector_2.toListSafe).map {
                case ((a, b), c) => (a, b, c)
              }
          )
        }
      }
    implicit val forSingletTypedGroup: OutputArguments[TypedGroupByExpression[Double]] =
      new OutputArguments[TypedGroupByExpression[Double]] {
        override type Result = Double
        override def allocateVectors()(implicit
          rootAllocator: RootAllocator
        ): (List[VectorOutputNativeArgument], () => List[Result]) = {
          val outVector_0 = new Float8Vector("output_0", rootAllocator)

          (List(NativeArgument.output(outVector_0)), () => outVector_0.toList)
        }
      }
  }

  trait ProjectExpression[Output] {
    def outputs(output: Output): List[Either[NamedStringExpression, NamedTypedCExpression]]
  }
  object ProjectExpression {
    implicit def fromScalar[O](implicit
      scalarProjectExpression: ScalarProjectExpression[O]
    ): ProjectExpression[O] =
      o => scalarProjectExpression.outputs(o).map(ne => Right(ne))
    implicit val forPairStringDouble
      : ProjectExpression[(StringProducer, TypedCExpression[Double])] =
      (output: (StringProducer, TypedCExpression[Double])) =>
        List(
          Left(NamedStringExpression("output_0", output._1)),
          Right(
            NamedTypedCExpression("output_1", VeScalarType.veNullableDouble, output._2.cExpression)
          )
        )
  }
  trait ScalarProjectExpression[Output] {
    def outputs(output: Output): List[NamedTypedCExpression]
  }

  object ScalarProjectExpression {
    implicit val forDouble: ScalarProjectExpression[TypedCExpression[Double]] =
      (output: TypedCExpression[Double]) =>
        List(NamedTypedCExpression("output_0", VeScalarType.veNullableDouble, output.cExpression))
    implicit val forPairDouble
      : ScalarProjectExpression[(TypedCExpression[Double], TypedCExpression[Double])] =
      (output: (TypedCExpression[Double], TypedCExpression[Double])) =>
        List(
          NamedTypedCExpression("output_0", VeScalarType.veNullableDouble, output._1.cExpression),
          NamedTypedCExpression("output_1", VeScalarType.veNullableDouble, output._2.cExpression)
        )
    implicit val forPairDoubleOneOption
      : ScalarProjectExpression[(TypedCExpression[Double], TypedCExpression[Option[Double]])] =
      (output: (TypedCExpression[Double], TypedCExpression[Option[Double]])) =>
        List(
          NamedTypedCExpression("output_0", VeScalarType.veNullableDouble, output._1.cExpression),
          NamedTypedCExpression("output_1", VeScalarType.veNullableDouble, output._2.cExpression)
        )
    implicit val forTripletDouble: ScalarProjectExpression[
      (TypedCExpression[Double], TypedCExpression[Double], TypedCExpression[Double])
    ] =
      (output: (TypedCExpression[Double], TypedCExpression[Double], TypedCExpression[Double])) =>
        List(
          NamedTypedCExpression("output_0", VeScalarType.veNullableDouble, output._1.cExpression),
          NamedTypedCExpression("output_1", VeScalarType.veNullableDouble, output._2.cExpression),
          NamedTypedCExpression("output_2", VeScalarType.veNullableDouble, output._3.cExpression)
        )
  }
  final case class TypedCExpression[ScalaType](cExpression: CExpression)

  trait JoinExpressor[Output] {
    def express(output: Output): List[NamedJoinExpression]
  }

  object JoinExpressor {
    implicit val forQuartetDouble: JoinExpressor[
      (
        TypedJoinExpression[Double],
        TypedJoinExpression[Double],
        TypedJoinExpression[Double],
        TypedJoinExpression[Double]
      )
    ] = output =>
      List(
        NamedJoinExpression("output_1", VeScalarType.veNullableDouble, output._1.joinExpression),
        NamedJoinExpression("output_2", VeScalarType.veNullableDouble, output._2.joinExpression),
        NamedJoinExpression("output_3", VeScalarType.veNullableDouble, output._3.joinExpression),
        NamedJoinExpression("output_4", VeScalarType.veNullableDouble, output._4.joinExpression)
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
        NamedJoinExpression("output_1", VeScalarType.veNullableDouble, output._1.joinExpression),
        NamedJoinExpression("output_2", VeScalarType.veNullableDouble, output._2.joinExpression),
        NamedJoinExpression("output_3", VeScalarType.veNullableDouble, output._3.joinExpression),
        NamedJoinExpression("output_4", VeScalarType.veNullableDouble, output._4.joinExpression)
      )
  }

  trait GeneralGroupExpressor[Output] {
    def express(output: Output): List[Either[NamedStringProducer, NamedGroupByExpression]]
  }
  object GeneralGroupExpressor {
    implicit val forTripletDouble
      : GeneralGroupExpressor[(StringGrouping, TypedGroupByExpression[Double])] = output =>
      List(
        Left(NamedStringProducer("output_0", StringProducer.copyString(output._1.name))),
        Right(
          NamedGroupByExpression(
            "output_1",
            VeScalarType.veNullableDouble,
            output._2.groupByExpression
          )
        )
      )
  }
  trait GroupExpressor[Output] {
    def express(output: Output): List[NamedGroupByExpression]
  }

  object GroupExpressor {
    implicit val forSinglet: GroupExpressor[TypedGroupByExpression[Double]] = output =>
      List(
        NamedGroupByExpression("output_0", VeScalarType.veNullableDouble, output.groupByExpression)
      )

    implicit val forTripletDouble: GroupExpressor[
      (
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Double]
      )
    ] = output =>
      List(
        NamedGroupByExpression(
          "output_0",
          VeScalarType.veNullableDouble,
          output._1.groupByExpression
        ),
        NamedGroupByExpression(
          "output_1",
          VeScalarType.veNullableDouble,
          output._2.groupByExpression
        ),
        NamedGroupByExpression(
          "output_2",
          VeScalarType.veNullableDouble,
          output._3.groupByExpression
        )
      )
    implicit val forTripletDoubleWOption: GroupExpressor[
      (
        TypedGroupByExpression[Option[Double]],
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Double]
      )
    ] = output =>
      List(
        NamedGroupByExpression(
          "output_0",
          VeScalarType.veNullableDouble,
          output._1.groupByExpression
        ),
        NamedGroupByExpression(
          "output_1",
          VeScalarType.veNullableDouble,
          output._2.groupByExpression
        ),
        NamedGroupByExpression(
          "output_2",
          VeScalarType.veNullableDouble,
          output._3.groupByExpression
        )
      )
    implicit val forTripletDoubleWOption2: GroupExpressor[
      (
        TypedGroupByExpression[Option[Double]],
        TypedGroupByExpression[Double],
        TypedGroupByExpression[Option[Double]]
      )
    ] = output =>
      List(
        NamedGroupByExpression(
          "output_0",
          VeScalarType.veNullableDouble,
          output._1.groupByExpression
        ),
        NamedGroupByExpression(
          "output_1",
          VeScalarType.veNullableDouble,
          output._2.groupByExpression
        ),
        NamedGroupByExpression(
          "output_2",
          VeScalarType.veNullableDouble,
          output._3.groupByExpression
        )
      )
  }

}
