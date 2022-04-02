package com.nec.native

import com.nec.spark.agile.core._
import com.nec.native.CppTranspiler.VeSignature
import scala.reflect.runtime.universe._
import java.time.Instant

object SyntaxTreeOps {
  /*
    NOTE: These extension methods to Function assume that the tree has been
    reformatted and type-annotated with `FunctionReformatter`!
  */

  implicit class ExtendedTreeFunction(func: Function) {
    def argTypes: Seq[Type] = {
      func.vparams.map(_.tpt.asInstanceOf[TypeTree].tpe)
    }

    def returnType: Type = {
      func.body match {
        case Literal(constant) => constant.tpe
        case _ => func.body.tpe
      }
    }

    def veSignature: VeSignature = {
      VeSignature(
        argTypes.toList.zipWithIndex.map { case (tpe, i) =>
          CVector(s"in_${i + 1}", tpe.toVeType)
        },
        List(returnType).zipWithIndex.map { case (tpe, i) =>
          CVector(s"out_$i", tpe.toVeType)
        }
      )
    }
  }

  implicit class ExtendedTreeType(tpe: Type) {
    def toVeType: VeType = {
      if (tpe =:= typeOf[Int]) {
        VeNullableInt
      } else if (tpe =:= typeOf[Long]) {
        VeNullableLong
      } else if (tpe =:= typeOf[Float]) {
        VeNullableFloat
      } else if (tpe =:= typeOf[Double]) {
        VeNullableDouble
      } else if (tpe =:= typeOf[Instant]) {
        VeNullableLong
      } else {
        throw new NotImplementedError(s"No corresponding VeType found for type ${tpe}")
      }
    }
  }
}
