package com.nec.cmake.eval

import com.nec.arrow.ArrowNativeInterface.NativeArgument
import com.nec.arrow.ArrowNativeInterface.NativeArgument.{
  VectorInputNativeArgument,
  VectorOutputNativeArgument
}
import com.nec.cmake.functions.ParseCSVSpec.RichFloat8
import com.nec.spark.agile.CFunctionGeneration.{CExpression, CVector, NamedTypedCExpression, VeType}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float8Vector

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

  trait InputArguments[Input] {
    def allocateVectors(data: Input*)(implicit
      rootAllocator: RootAllocator
    ): List[VectorInputNativeArgument]
    def inputs: List[CVector]
  }
  object InputArguments {

    implicit val forDouble: InputArguments[Double] = new InputArguments[Double] {
      override def allocateVectors(
        data: Double*
      )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
        inputs.zipWithIndex.map { case (CVector(name, tpe), idx) =>
          val vcv = new Float8Vector(name, rootAllocator)
          vcv.allocateNew()
          vcv.setValueCount(data.size)
          data.zipWithIndex.foreach { case (str, idx) =>
            vcv.setSafe(idx, str)
          }
          NativeArgument.input(vcv)
        }
      }

      override def inputs: List[CVector] = List(CVector("input_0", VeType.veNullableDouble))
    }
    implicit val forPairDouble: InputArguments[(Double, Double)] =
      new InputArguments[(Double, Double)] {
        override def allocateVectors(
          data: (Double, Double)*
        )(implicit rootAllocator: RootAllocator): List[VectorInputNativeArgument] = {
          inputs.zipWithIndex.map { case (CVector(name, tpe), idx_col) =>
            val vcv = new Float8Vector(name, rootAllocator)
            vcv.allocateNew()
            vcv.setValueCount(data.size)
            data.zipWithIndex.foreach { case ((first, second), idx) =>
              vcv.setSafe(idx, if (idx_col == 0) first else second)
            }
            NativeArgument.input(vcv)
          }
        }

        override def inputs: List[CVector] = List(
          CVector("input_0", VeType.veNullableDouble),
          CVector("input_1", VeType.veNullableDouble)
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
  }

  trait ProjectExpression[Output] {
    def outputs(output: Output): List[NamedTypedCExpression]
  }

  object ProjectExpression {
    implicit val forDouble: ProjectExpression[TypedCExpression[Double]] =
      (output: TypedCExpression[Double]) =>
        List(NamedTypedCExpression("output_0", VeType.veNullableDouble, output.cExpression))
    implicit val forPairDouble
      : ProjectExpression[(TypedCExpression[Double], TypedCExpression[Double])] =
      (output: (TypedCExpression[Double], TypedCExpression[Double])) =>
        List(
          NamedTypedCExpression("output_0", VeType.veNullableDouble, output._1.cExpression),
          NamedTypedCExpression("output_1", VeType.veNullableDouble, output._2.cExpression)
        )
    implicit val forPairDoubleOneOption
      : ProjectExpression[(TypedCExpression[Double], TypedCExpression[Option[Double]])] =
      (output: (TypedCExpression[Double], TypedCExpression[Option[Double]])) =>
        List(
          NamedTypedCExpression("output_0", VeType.veNullableDouble, output._1.cExpression),
          NamedTypedCExpression("output_1", VeType.veNullableDouble, output._2.cExpression)
        )
  }
  final case class TypedCExpression[ScalaType](cExpression: CExpression)
}
