package io.sparkcyclone.spark.codegen.projection

import io.sparkcyclone.spark.codegen.VeFunctionTemplate
import io.sparkcyclone.spark.codegen.CFunctionGeneration._
import io.sparkcyclone.spark.codegen.StringProducer.FrovedisStringProducer
import io.sparkcyclone.native.code._
import io.sparkcyclone.native.code.CFunction2.CFunctionArgument

final case class ProjectionFunction(name: String,
                                    data: Seq[CVector],
                                    expressions: Seq[Either[NamedStringExpression, NamedTypedCExpression]])
                                    extends VeFunctionTemplate {
  require(data.nonEmpty, "Expected Projection to have at least one data column")
  require(expressions.nonEmpty, "Expected Projection to have at least one projection expression")

  lazy val inputs: Seq[CVector] = {
    data
  }

  lazy val outputs: Seq[CVector] = {
    expressions.map {
      case Right(NamedTypedCExpression(name, vetype, _)) =>
        CScalarVector(name, vetype)

      case Left(NamedStringExpression(name, _)) =>
        CVarChar(name)
    }
  }

  lazy val arguments: Seq[CFunction2.CFunctionArgument] = {
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

  def hashId: Int = {
    /*
      The semantic identity of the SortFunction will be determined by the
      data columns and projection expressions.
    */
    (getClass.getName, data.map(_.veType), expressions).hashCode
  }

  def toCFunction: CFunction2 = {
    CFunction2(
      name,
      arguments,
      CodeLines.from(inputPtrDeclStmts, "", outputPtrDeclStmts, "", expressions.map(projectionStmt))
    )
  }

  def secondary: Seq[CFunction2] = {
    Seq.empty
  }
}
