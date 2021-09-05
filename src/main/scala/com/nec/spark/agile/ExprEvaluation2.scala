package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines

/** Spark-free function evaluation */
object ExprEvaluation2 {

  final case class CVector(name: String, veType: VeType)

  final case class CExpression(cCode: String, isNotNullCode: Option[String])

  final case class TypedCExpression(veType: VeType, cExpression: CExpression)

  final case class NamedTypedCExpression(name: String, veType: VeType, cExpression: CExpression)

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

    case object VeNullableInt extends VeType {
      def cType: String = "int"

      def cVectorType: String = "nullable_int_vector"

      override def cSize: Int = 4
    }

    def veDouble: VeType = VeNullableDouble
  }

  /** The reason to use fully generic types is so that we can map them around in future, without having an implementation
   * that interferes with those changes. By 'manipulate' we mean optimize/find shortcuts/etc.
   *
   * By doing this, we flatten the code hierarchy and can now do validation of C behaviors without requiring Spark to be pulled in.
   *
   * This even enables us the possibility to use Frovedis behind the scenes.
   * */
  final case class VeProjection[Input, Output](inputs: List[Input], outputs: List[Output])

  final case class VeFilter[Data, Condition](data: List[Data], condition: Condition)

  def generateFilter(filter: VeFilter[CVector, CExpression]): CodeLines = {
    CodeLines.from(
      filter.data.map { case CVector(name, veType) =>
        s"std::vector<${veType.cType}> filtered_$name = {};"
      },
      "for ( long i = 0; i < input_0->count; i++ ) {",
      s"if ( ${filter.condition.cCode} ) {"
      , filter.data.map {
        case CVector(name, _) =>
          s"  filtered_$name.push_back($name->data[i]);"
      }, "}", "}",
      filter.data.map { case CVector(name, veType) =>
        CodeLines.empty
          .append(
            s"memcpy($name->data, filtered_$name.data(), filtered_$name.size() * ${veType.cSize});",
            s"$name->count = filtered_$name.size();",
            // this causes a crash - what are we doing wrong here?
            //          s"realloc(input_$i->data, input_$i->count * 8);",
            s"filtered_$name.clear();"
          )
      }
    )
  }

  def renderFilter(functionName: String, filter: VeFilter[CVector, CExpression]): CodeLines = {
    val filterOutput = filter.data.map {
      case CVector(name, veType) =>
        CVector(name.replaceAllLiterally("input", "output"), veType)
    }

    CodeLines.from("#include <cmath>",
      "#include <bitset>",
      "#include <iostream>",
      s"""extern "C" long $functionName(""", {
        filter.data.map { case CVector(name, veType) =>
          s"${veType.cVectorType} *$name"
        } ++
          filterOutput.map { case CVector(name, veType) =>
            s"${veType.cVectorType} *$name"
          }
      }.mkString(",\n"),
      ") {",
      generateFilter(filter),
      filterOutput.map {
        case CVector(outputName, outputVeType) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->data = (${outputVeType.cType}*) malloc($outputName->count * sizeof(${outputVeType.cSize}));"
          )
      }
      , "for ( long i = 0; i < input_0->count; i++ ) {",
      filter.data.zip(filterOutput).map {
        case (CVector(inputName, inputVeType), CVector(outputName, outputVeType)) =>
          s"""$outputName->data[i] = $inputName->data[i];"""
      },
      "}",
      filter.data.zip(filterOutput).map {
        case (CVector(inputName, inputVeType), CVector(outputName, outputVeType)) =>
          s"""$outputName->validityBuffer = $inputName->validityBuffer;"""
      },
      "return 0;", "}"
    )
  }

  def renderProjection(functionName: String, veDataTransformation: VeProjection[CVector, NamedTypedCExpression]): CodeLines = {
    CodeLines.from("#include <cmath>",
      "#include <bitset>",
      "#include <iostream>",
      s"""extern "C" long $functionName(""", {
        veDataTransformation.inputs.map { case CVector(name, veType) =>
          s"${veType.cVectorType} *$name"
        } ++
          veDataTransformation.outputs.zipWithIndex.map { case (NamedTypedCExpression(outputName, veType, _), idx) =>
            s"${veType.cVectorType} *output_${idx}"
          }
      }.mkString(",\n"),
      ") {",
      veDataTransformation.outputs.zipWithIndex.map {
        case (NamedTypedCExpression(outputName, veType, _), idx) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->data = (${veType.cType}*) malloc($outputName->count * sizeof(${veType.cSize}));",
            s"$outputName->validityBuffer = (unsigned char *) malloc(ceil($outputName->count / 8.0));",
          )
      }
      , "for ( long i = 0; i < input_0->count; i++ ) {",
      veDataTransformation.outputs.zipWithIndex.map {
        case (NamedTypedCExpression(outputName, veType, cExpr), idx) =>
          cExpr.isNotNullCode match {
            case None =>
              CodeLines.from(
                s"""$outputName->data[i] = ${cExpr.cCode};""",
                s"set_validity($outputName->validityBuffer, i, 1);"
              )
            case Some(notNullCheck) =>
              CodeLines.from(
                s"if ( $notNullCheck ) {",
                s"""$outputName->data[i] = ${cExpr.cCode};""",
                s"set_validity($outputName->validityBuffer, i, 1);",
                "} else {",
                s"set_validity($outputName->validityBuffer, i, 0);",
                "}"
              )
          }

      },
      "}",
      "return 0;", "}"
    )
  }

}
