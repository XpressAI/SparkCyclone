package com.nec.spark.agile.projection

import com.nec.spark.agile.core._
import com.nec.spark.agile.CFunctionGeneration._
import com.nec.spark.agile.StringProducer.FrovedisStringProducer
import com.nec.spark.agile.core.{CFunction2, FunctionTemplateTrait}
import com.nec.spark.agile.core.CFunction2.CFunctionArgument


case class ProjectionFunction(name: String,
                              data: List[CVector],
                              expressions: List[Either[NamedStringExpression, NamedTypedCExpression]])
                              extends FunctionTemplateTrait {
  require(data.nonEmpty, "Expected Projection to have at least one data column")
  require(expressions.nonEmpty, "Expected Projection to have at least one projection expression")

  lazy val inputs: List[CVector] = {
    data
  }

  lazy val outputs: List[CVector] = {
    expressions.map {
      case Right(NamedTypedCExpression(name, vetype, _)) =>
        CScalarVector(name, vetype)

      case Left(NamedStringExpression(name, _)) =>
        CVarChar(name)
    }
  }

  lazy val arguments: List[CFunction2.CFunctionArgument] = {
    inputs.map { vec => CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_m")) } ++
      outputs.map { vec => CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_mo")) }
  }

  private[projection] def inputPtrDeclStmts: CodeLines = {
    inputs.map { input =>
      s"const auto *${input.name} = ${input.name}_m[0];"
    }
  }

  private[projection] def outputPtrDeclStmts: CodeLines = {
    outputs.map { output =>
      CodeLines.from(
        s"auto *${output.name} = ${output.veType.cVectorType}::allocate();",
        s"*${output.name}_mo = ${output.name};"
      )
    }
  }

  private[projection] def projectionStmt(expression: Either[NamedStringExpression, NamedTypedCExpression]): CodeLines = {
    expression match {
      case Left(NamedStringExpression(outname, producer: FrovedisStringProducer)) =>
        producer.produce(outname, s"${inputs.head.name}->count", "i")

      case Right(NamedTypedCExpression(outname, vetype, cexpr)) =>
        CodeLines.scoped(s"Project onto ${outname}") {
          CodeLines.from(
            s"${outname}->resize(${inputs.head.name}->count);",
            "#pragma _NEC vector",
            CodeLines.forLoop("i", s"${inputs.head.name}->count") {
              List(
                s"bool validity = ${cexpr.isNotNullCode.getOrElse("1")};",
                s"$outname->data[i] = ${cexpr.cCode};",
                s"$outname->set_validity(i, validity);"
              )
            }
          )
        }
    }
  }

  def toCFunction: CFunction2 = {
    CFunction2(
      name,
      arguments,
      CodeLines.from(inputPtrDeclStmts, "", outputPtrDeclStmts, "", expressions.map(projectionStmt))
    )
  }
}
