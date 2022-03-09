package com.nec.ve

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunction2
import com.nec.spark.agile.CFunction2.CFunctionArgument
import com.nec.spark.agile.CFunctionGeneration.{
  CScalarVector,
  CVarChar,
  CVector,
  NamedStringExpression,
  NamedTypedCExpression
}
import com.nec.spark.agile.StringProducer.FrovedisStringProducer

case class ProjectionFunction(
  name: String,
  data: List[CVector],
  expressions: List[Either[NamedStringExpression, NamedTypedCExpression]]
) {
  require(data.nonEmpty, "Expected Projection to have at least one data column")
  require(expressions.nonEmpty, "Expected Projection to have at least one projection expression")

  lazy val inputs: List[CVector] = {
    data.map { vec => vec.withNewName(s"${vec.name}_m") }
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
    inputs.map(CFunctionArgument.PointerPointer(_)) ++
      outputs.map { vec => CFunctionArgument.PointerPointer(vec.withNewName(s"${vec.name}_mo")) }
  }

  private[ve] def inputPtrDeclStmts: CodeLines = {
    (data, inputs).zipped.map { case (dvec, ivec) =>
      s"${dvec.declarePointer} = ${ivec.name}[0];"
    }
  }

  private[ve] def outputPtrDeclStmts: CodeLines = {
    outputs.map { ovec =>
      CodeLines.from(
        s"${ovec.declarePointer} = ${ovec.veType.cVectorType}::allocate();",
        s"*${ovec.name}_mo = ${ovec.name};"
      )
    }
  }

  def projectionStmt(
    expression: Either[NamedStringExpression, NamedTypedCExpression]
  ): CodeLines = {
    expression match {
      case Left(NamedStringExpression(outname, producer: FrovedisStringProducer)) =>
        producer.produce(outname, s"${data.head.name}->count", "i")

      case Right(NamedTypedCExpression(outname, vetype, cexpr)) =>
        CodeLines.scoped(s"Project onto ${outname}") {
          CodeLines.from(
            s"${outname}->resize(${data.head.name}->count);",
            "#pragma _NEC vector",
            CodeLines.forLoop("i", s"${data.head.name}->count") {
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

  def render: CFunction2 = {
    CFunction2(
      name,
      arguments,
      CodeLines.from(inputPtrDeclStmts, "", outputPtrDeclStmts, "", expressions.map(projectionStmt))
    )
  }

  def toCodeLines: CodeLines = {
    render.toCodeLines
  }
}
