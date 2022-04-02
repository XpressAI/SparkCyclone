package com.nec.util

import com.nec.spark.agile.core._

import java.time.Instant
import scala.reflect.runtime.universe._

object SyntaxTreeOps {
  /*
    Extension tree functions require the Tree to have been type-annotated with
    `toolbox.typecheck()`
  */

  implicit class ExtendedTreeFunction(func: Function) {
    def argTypes: Seq[Type] = {
      func.vparams.map(_.tpt.asInstanceOf[TypeTree].tpe)
    }

    def returnType: Type = {
      func.body.tpe
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
