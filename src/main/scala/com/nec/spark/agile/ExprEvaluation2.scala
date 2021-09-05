package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.{CodeLines, NameCleaner, RichListStr, cGenProject}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, LessThan, Literal}
import org.apache.spark.sql.types.{DoubleType, Metadata}

object ExprEvaluation2 {

  sealed trait VeType {
    def cType: String

    def cSize: Int

    def cVectorType: String
  }

  object VeType {
    case object VeNullableDouble extends VeType {
      def cType: String = "double"

      def cVectorType: String = "nullable_double_vector"

      override def cSize: Int = 8
    }

    def veDouble: VeType = VeNullableDouble
  }

  final case class VeDataTransformation[Data, Process](input: Data, output: Data, process: Process) {
  }

  final case class Filter[T](data: T)

  final case class CVector(name: String, veType: VeType)

  final case class CExpression(cCode: String)

  def renderCode(functionName: String, veDataTransformation: VeDataTransformation[List[CVector], Filter[CExpression]]): CodeLines = {
    CodeLines.from("#include <cmath>",
      "#include <bitset>",
      "#include <iostream>",
      s"""extern "C" long $functionName(""", {
        veDataTransformation.input.map { case CVector(name, veType) =>
          s"${veType.cVectorType} *$name"
        } ++
          veDataTransformation.output.map { case CVector(name, veType) =>
            s"${veType.cVectorType} *$name"
          }
      }.mkString(",\n"),
      ") {",


      veDataTransformation.input.map { case CVector(name, veType) =>
        s"std::vector<${veType.cType}> filtered_$name = {};"
      },
      "for ( long i = 0; i < input_0->count; i++ ) {",
      s"if ( ${veDataTransformation.process.data.cCode} ) {"
      , veDataTransformation.input.map {
        case CVector(name, _) =>
          s"  filtered_$name.push_back($name->data[i]);"
      }, "}", "}",
      veDataTransformation.input.map { case CVector(name, veType) =>
        CodeLines.empty
          .append(
            s"memcpy($name->data, filtered_$name.data(), filtered_$name.size() * ${veType.cSize});",
            s"$name->count = filtered_$name.size();",
            // this causes a crash - what are we doing wrong here?
            //          s"realloc(input_$i->data, input_$i->count * 8);",
            s"filtered_$name.clear();"
          )
      },
      veDataTransformation.output.map {
        case CVector(outputName, outputVeType) =>
          CodeLines.from(
            s"${outputName}->count = input_0->count;",
            s"${outputName}->data = (${outputVeType.cType}*) malloc(${outputName}->count * sizeof(${outputVeType.cSize}));"
          )
      }
      , "for ( long i = 0; i < input_0->count; i++ ) {",
      veDataTransformation.input.zip(veDataTransformation.output).map {
        case (CVector(inputName, inputVeType), CVector(outputName, outputVeType)) =>
          s"""${outputName}->data[i] = ${inputName}->data[i];"""
      },
      "}",
      veDataTransformation.input.zip(veDataTransformation.output).map {
        case (CVector(inputName, inputVeType), CVector(outputName, outputVeType)) =>
          s"""${outputName}->validityBuffer = ${inputName}->validityBuffer;"""
      },
      "return 0;", "}"
    )
  }

  private val ref_value14 =
    AttributeReference(
      name = "value#14",
      dataType = DoubleType,
      nullable = false,
      metadata = Metadata.empty
    )()

  def filterDouble: CodeLines = {
    implicit val nameCleaner = NameCleaner.simple

    renderCode(functionName = "filter_f", veDataTransformation = VeDataTransformation(
      input = List(CVector("input_0", VeType.veDouble)),
      output = List(CVector("output_0", VeType.veDouble)),
      process = Filter(CExpression("input_0->data[i] > 15"))
    ))

    /*cGenProject(
      fName = "filter_f",
      inputReferences = Set("value#14", "value#15"),
      childOutputs = Seq(ref_value14),
      resultExpressions = Seq(ref_value14),
      maybeFilter = Some(
        LessThan(ref_value14, Literal(15))
      ),
    )*/
  }

  def filterDouble2: CodeLines = {
    implicit val nameCleaner = NameCleaner.simple
    cGenProject(
      fName = "filter_f",
      inputReferences = Set("value#14", "value#15"),
      childOutputs = Seq(ref_value14),
      resultExpressions = Seq(ref_value14),
      maybeFilter = Some(
        LessThan(ref_value14, Literal(15))
      ),
    )
  }
}
