package com.nec.native.transpiler

import com.nec.spark.agile.core._
import scala.reflect.runtime.universe._
import java.time.Instant

case class VeSignature(inputs: List[CVector], outputs: List[CVector])

object SyntaxTreeOps {
  /*
    NOTE: All extension methods to Function assume that the tree has been
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

    def veReduceSignature: VeSignature = {
      val inParamCount = func.vparams.size / 2
      val inOut = func.argTypes.take(inParamCount)
      VeSignature(
        inOut.zipWithIndex.map{ case (t, i) => CVector(s"in_${i + 1}", t.toVeType)}.toList,
        inOut.zipWithIndex.map{ case (t, i) => CVector(s"out_$i", t.toVeType)}.toList
      )
    }

    def aggregateParams: List[ValDef] = {
      val inParamCount = func.vparams.size / 2
      func.vparams.drop(inParamCount)
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
      } else if (tpe =:= typeOf[Short]) {
        // Shorts are represented as Ints on the VE to enable vectorization
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
