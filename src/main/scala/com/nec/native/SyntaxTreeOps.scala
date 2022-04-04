package com.nec.native

import com.nec.native.CppTranspiler.VeSignature
import com.nec.spark.agile.core._

import java.time.Instant
import scala.reflect.runtime.universe._

object SyntaxTreeOps {
  /*
    NOTE: These extension methods to Function assume that the tree has been
    reformatted and type-annotated with `FunctionReformatter`!
  */

  def paramNameGenerator: String => String = (i: String) => s"in${i}_val"

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

    def veMapSignature: VeSignature = {
      VeSignature(
        argTypes.toList.zipWithIndex.map { case (tpe, i) =>
          CVector(s"in_${i + 1}", tpe.toVeType)
        },
        returnType.toVeTypes.zipWithIndex.map { case (veType, i) =>
          CVector(s"out_$i", veType)
        }
      )
    }

    def veInOutSignature: VeSignature = {
      VeSignature(
        argTypes.toList.zipWithIndex.map { case (tpe, i) =>
          CVector(s"in_${i + 1}", tpe.toVeType)
        },
        argTypes.toList.zipWithIndex.map { case (veType, i) =>
          CVector(s"out_$i", veType.toVeType)
        }
      )
    }
  }

  implicit class ExtendedTreeType(tpe: Type) {
    def toVeTypes: List[VeType] = {
      tpe.asInstanceOf[TypeRef].args match {
        case Nil => List(toVeType)
        case args => args.map(a => a.toVeType)
      }
    }

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
      } else if (tpe =:= typeOf[Boolean]) {
        VeNullableInt // Should we have a boolean type instead?
      } else {
        throw new NotImplementedError(s"No corresponding VeType found for type ${tpe}")
      }
    }
  }
}
