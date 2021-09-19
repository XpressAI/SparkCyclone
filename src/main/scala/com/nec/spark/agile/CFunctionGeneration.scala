package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.{CodeLines, cType, cTypeOfSub, evaluateExpression}

/** Spark-free function evaluation */
object CFunctionGeneration {

  final case class CVector(name: String, veType: VeType)

  final case class CExpression(cCode: String, isNotNullCode: Option[String])
  final case class CExpressionWithCount(cCode: String, isNotNullCode: Option[String])

  object CExpression
  final case class TypedCExpression2(veType: VeType, cExpression: CExpression)
  final case class NamedTypedCExpression(name: String, veType: VeType, cExpression: CExpression)

  sealed trait VeType {
    def cScalarType: String

    def cSize: Int

    def cVectorType: String
  }

  object VeType {
    case object VeNullableDouble extends VeType {
      def cScalarType: String = "double"

      def cVectorType: String = "nullable_double_vector"

      override def cSize: Int = 8
    }

    case object VeNullableInt extends VeType {
      def cScalarType: String = "int"

      def cVectorType: String = "nullable_int_vector"

      override def cSize: Int = 4
    }

    case object VeNullableLong extends VeType {
      def cScalarType: String = "long long"

      def cVectorType: String = "nullable_bigint_vector"

      override def cSize: Int = 8
    }

    def veNullableDouble: VeType = VeNullableDouble
    def veNullableInt: VeType = VeNullableInt
    def veNullableLong: VeType = VeNullableLong
  }

  /**
   * The reason to use fully generic types is so that we can map them around in future, without having an implementation
   * that interferes with those changes. By 'manipulate' we mean optimize/find shortcuts/etc.
   *
   * By doing this, we flatten the code hierarchy and can now do validation of C behaviors without requiring Spark to be pulled in.
   *
   * This even enables us the possibility to use Frovedis behind the scenes.
   */
  final case class VeProjection[Input, Output](inputs: List[Input], outputs: List[Output])

  final case class VeGroupBy[Input, Group, Output](
    inputs: List[Input],
    groups: List[Group],
    outputs: List[Output]
  )
  sealed trait JoinType
  case object LeftOuterJoin extends JoinType
  case object RightOuterJoin extends JoinType

  final case class VeInnerJoin[Input, LeftKey, RightKey, Output](
                                                    inputs: List[Input],
                                                    leftKey: LeftKey,
                                                    rightKey: RightKey,
                                                    outputs: List[Output]
                                                  )

  final case class OuterJoinOutput[Output](innerJoinOutputs: Output, outerJoinOutputs: Output)

  final case class VeOuterJoin[Input, LeftKey, RightKey, Output](
                                                                  inputs: List[Input],
                                                                  leftKey: LeftKey,
                                                                  rightKey: RightKey,
                                                                  outputs: List[OuterJoinOutput[Output]],
                                                                  joinType: JoinType
                                                                )
  sealed trait JoinExpression {
    def fold[T](whenProj: CExpression => T): T
  }
  final case class TypedJoinExpression[ScalaType](joinExpression: JoinExpression)

  object JoinExpression {
    final case class JoinProjection(cExpression: CExpression) extends JoinExpression {
      override def fold[T](whenProj: CExpression => T): T = whenProj(
        cExpression
      )
    }
    //TODO: We can use that to meld join and aggregate
//    final case class JoinAggregation(aggregation: Aggregation) extends JoinExpression {
//      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenAgg(
//        aggregation
//      )
//    }
  }
  final case class NamedJoinExpression(
                                           name: String,
                                           veType: VeType,
                                           joinExpression: JoinExpression
                                         )

  final case class TypedGroupByExpression[ScalaType](groupByExpression: GroupByExpression)

  sealed trait GroupByExpression {
    def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T
  }
  object GroupByExpression {
    final case class GroupByProjection(cExpression: CExpression) extends GroupByExpression {
      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenProj(
        cExpression
      )
    }
    final case class GroupByAggregation(aggregation: Aggregation) extends GroupByExpression {
      override def fold[T](whenProj: CExpression => T, whenAgg: Aggregation => T): T = whenAgg(
        aggregation
      )
    }
  }

  final case class NamedGroupByExpression(
    name: String,
    veType: VeType,
    groupByExpression: GroupByExpression
  )

  trait Aggregation extends Serializable {
    def initial(prefix: String): CodeLines
    def iterate(prefix: String): CodeLines
    def compute(prefix: String): CodeLines
    def fetch(prefix: String): CExpression
    def free(prefix: String): CodeLines
  }

  object Aggregation {
    def sum(cExpression: CExpression): Aggregation = new Aggregation {
      override def initial(prefix: String): CodeLines =
        CodeLines.from(s"double ${prefix}_aggregate_sum = 0;")

      override def iterate(prefix: String): CodeLines =
        cExpression.isNotNullCode match {
          case None =>
            CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};")
          case Some(notNullCheck) =>
            CodeLines.from(
              s"if ( ${notNullCheck} ) {",
              CodeLines.from(s"${prefix}_aggregate_sum += ${cExpression.cCode};").indented,
              "}"
            )
        }

      override def fetch(prefix: String): CExpression =
        CExpression(s"${prefix}_aggregate_sum", None)

      override def free(prefix: String): CodeLines = CodeLines.empty

      override def compute(prefix: String): CodeLines = CodeLines.empty
    }

    def corr(x: CExpression, y: CExpression): Aggregation = new Aggregation {
      override def initial(prefix: String): CodeLines =
        CodeLines.from(
          s"double ${prefix}_corr_count = 0;",
          s"double ${prefix}_corr_x_sum = 0;",
          s"double ${prefix}_corr_y_sum = 0;",
          s"double ${prefix}_corr_xy_sum = 0;",
          s"double ${prefix}_corr_x_square_sum = 0;",
          s"double ${prefix}_corr_y_square_sum = 0;"
        )

      override def iterate(prefix: String): CodeLines =
        CodeLines.from(
          s"${prefix}_corr_count += 1;",
          s"${prefix}_corr_x_sum += ${x.cCode};",
          s"${prefix}_corr_y_sum += ${y.cCode};",
          s"${prefix}_corr_xy_sum += ${x.cCode} * ${y.cCode};",
          s"${prefix}_corr_x_square_sum += ${x.cCode} * ${x.cCode};",
          s"${prefix}_corr_y_square_sum +=  ${y.cCode} * ${y.cCode};"
        )

      override def fetch(prefix: String): CExpression =
        CExpression(
          s"(${prefix}_corr_count * ${prefix}_corr_xy_sum - ${prefix}_corr_x_sum * ${prefix}_corr_y_sum) / " +
            s"sqrt(" +
            s"(${prefix}_corr_count * ${prefix}_corr_x_square_sum - ${prefix}_corr_x_sum * ${prefix}_corr_x_sum) * " +
            s"(${prefix}_corr_count * ${prefix}_corr_y_square_sum - ${prefix}_corr_y_sum * ${prefix}_corr_y_sum));",
          None
        )

      override def free(prefix: String): CodeLines = CodeLines.empty

      override def compute(prefix: String): CodeLines = CodeLines.empty
    }
  }

  final case class VeFilter[Data, Condition](data: List[Data], condition: Condition)

  final case class VeSort[Data, Sort](data: List[Data], sorts: List[Sort])


  final case class CFunction(inputs: List[CVector], outputs: List[CVector], body: CodeLines) {
    def arguments: List[CVector] = inputs ++ outputs

    def toCodeLines(functionName: String): CodeLines = {
      CodeLines.from(
        "#include <cmath>",
        "#include <bitset>",
        "#include <iostream>",
        "#include <tuple>",
        "#include \"tuple_hash.hpp\"",
        """#include "frovedis/core/radix_sort.hpp"""",
        """#include "frovedis/dataframe/join.hpp"""",
        """#include "frovedis/dataframe/join.cc"""",
        """#include "frovedis/core/set_operations.hpp"""",
        s"""extern "C" long $functionName(""",
        arguments
          .map { case CVector(name, veType) =>
            s"${veType.cVectorType} *$name"
          }
          .mkString(",\n"),
        ") {",
        body.indented,
        "  ",
        "  return 0;",
        "};"
      )
    }
  }

  def generateFilter(filter: VeFilter[CVector, CExpression]): CodeLines = {
    CodeLines.from(
      filter.data.map { case CVector(name, veType) =>
        s"std::vector<${veType.cScalarType}> filtered_$name = {};"
      },
      "for ( long i = 0; i < input_0->count; i++ ) {",
      s"if ( ${filter.condition.cCode} ) {",
      filter.data.map { case CVector(name, _) =>
        s"  filtered_$name.push_back($name->data[i]);"
      },
      "}",
      "}",
      filter.data.map { case CVector(name, veType) =>
        CodeLines.empty
          .append(
            s"memcpy($name->data, filtered_$name.data(), filtered_$name.size() * sizeof(${veType.cScalarType}));",
            s"$name->count = filtered_$name.size();",
            // this causes a crash - what are we doing wrong here?
            //          s"realloc(input_$i->data, input_$i->count * 8);",
            s"filtered_$name.clear();"
          )
      }
    )
  }

  def renderSort(sort: VeSort[CVector, CExpression]): CFunction = {
    val sortOutput = sort.data.map { case CVector(name, veType) =>
      CVector(name.replaceAllLiterally("input", "output"), veType)
    }
    CFunction(
      inputs = sort.data,
      outputs = sortOutput,
      body = CodeLines.from(
        sortOutput.map { case CVector(outputName, outputVeType) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->validityBuffer = (unsigned char *) malloc(ceil($outputName->count / 8.0));",
            s"$outputName->data = (${outputVeType.cScalarType}*) malloc($outputName->count * sizeof(${outputVeType.cScalarType}));"
          )
        },
        "// create an array of indices, which by default are in order, but afterwards are out of order.",
        s"std::vector<size_t> idx(input_0->count);",
        "for (size_t i = 0; i < input_0->count; i++) {",
        "  idx[i] = i;",
        "}",
        "// create a 'grouping_vec', on which we will be able to do the radix sort",
        "std::vector<double> grouping_vec(input_1->data, input_1->data + input_1->count);",
        "frovedis::radix_sort(grouping_vec, idx, input_0->count);",
        "// prevent deallocation of input vector -- it is deallocated by the caller",
        "new (&grouping_vec) std::vector<double>;",
        s"for(int i = 0; i < input_0->count; i++) {",
        sort.data.zip(sortOutput).map { case (CVector(inName, veType), CVector(outputName, _)) =>
          CodeLines
            .from(
              s"$outputName->data[i] = $inName->data[idx[i]];",
              s"set_validity($outputName->validityBuffer, i, 1);"
            )
            .indented
        },
        "}"
      )
    )
  }

  def renderFilter(filter: VeFilter[CVector, CExpression]): CFunction = {
    val filterOutput = filter.data.map { case CVector(name, veType) =>
      CVector(name.replaceAllLiterally("input", "output"), veType)
    }

    CFunction(
      inputs = filter.data,
      outputs = filterOutput,
      body = CodeLines.from(
        generateFilter(filter),
        filterOutput.map { case CVector(outputName, outputVeType) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->validityBuffer = (unsigned char *) malloc(ceil($outputName->count / 8.0));",
            s"$outputName->data = (${outputVeType.cScalarType}*) malloc($outputName->count * sizeof(${outputVeType.cScalarType}));"
          )
        },
        "for ( long i = 0; i < input_0->count; i++ ) {",
        filter.data.zip(filterOutput).map {
          case (CVector(inputName, inputVeType), CVector(outputName, outputVeType)) =>
            CodeLines
              .from(
                s"""$outputName->data[i] = $inputName->data[i];""",
                s"""set_validity($outputName->validityBuffer, i, 1);"""
              )
              .indented
        },
        "}"
      )
    )
  }

  def renderProjection(
    veDataTransformation: VeProjection[CVector, NamedTypedCExpression]
  ): CFunction = CFunction(
    inputs = veDataTransformation.inputs,
    outputs = veDataTransformation.outputs.zipWithIndex.map {
      case (NamedTypedCExpression(outputName, veType, _), idx) =>
        CVector(outputName, veType)
    },
    body = CodeLines.from(
      veDataTransformation.outputs.zipWithIndex.map {
        case (NamedTypedCExpression(outputName, veType, _), idx) =>
          CodeLines.from(
            s"$outputName->count = input_0->count;",
            s"$outputName->data = (${veType.cScalarType}*) malloc($outputName->count * sizeof(${veType.cScalarType}));",
            s"$outputName->validityBuffer = (unsigned char *) malloc(ceil($outputName->count / 8.0));"
          )
      },
      "for ( long i = 0; i < input_0->count; i++ ) {",
      veDataTransformation.outputs.zipWithIndex
        .map { case (NamedTypedCExpression(outputName, veType, cExpr), idx) =>
          cExpr.isNotNullCode match {
            case None =>
              CodeLines.from(
                s"""$outputName->data[i] = ${cExpr.cCode};""",
                s"set_validity($outputName->validityBuffer, i, 1);"
              )
            case Some(notNullCheck) =>
              CodeLines.from(
                s"if ( $notNullCheck ) {",
                s"""  $outputName->data[i] = ${cExpr.cCode};""",
                s"  set_validity($outputName->validityBuffer, i, 1);",
                "} else {",
                s"  set_validity($outputName->validityBuffer, i, 0);",
                "}"
              )
          }

        }
        .map(_.indented),
      "}"
    )
  )

  val GroupBeforeSort = "before we can group we need to sort"

  def renderInnerJoin(
                     veInnerJoin: VeInnerJoin[CVector, TypedCExpression2, TypedCExpression2, NamedJoinExpression]
                     ): CFunction = {

    CFunction(
      inputs = veInnerJoin.inputs,
      outputs = veInnerJoin.outputs.zipWithIndex.map {
        case (NamedJoinExpression(outputName, veType, _), idx) =>
          CVector(outputName, veType)
      },
      body = CodeLines.from(
        s"std::vector <${veInnerJoin.leftKey.veType.cScalarType}> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <${veInnerJoin.rightKey.veType.cScalarType}> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        "for(int i = 0; i < input_0->count; i++) { ",
        CodeLines.from(
          s"left_vec.push_back(${veInnerJoin.leftKey.cExpression.cCode});",
          "left_idx.push_back(i);",
          s"right_vec.push_back(${veInnerJoin.rightKey.cExpression.cCode});",
          "right_idx.push_back(i);",
        ).indented,
        "}",
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        s"frovedis::equi_join<${veInnerJoin.leftKey.veType.cScalarType}>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);",
        "long validityBuffSize = ceil(left_out.size() / 8.0);",
        veInnerJoin.outputs.map {
          case NamedJoinExpression(outputName, veType, joinExpression) =>
            joinExpression.fold(
              whenProj = _ => CodeLines.from(
                s"${outputName}->data = (${veType.cScalarType}*) malloc(left_out.size() * sizeof(${veType.cScalarType}));",
                s"${outputName}->validityBuffer = (unsigned char *) malloc(validityBuffSize * sizeof(unsigned char*));"
              ))
        },

        "for(int i = 0; i < left_out.size(); i++) { ",
        veInnerJoin.outputs.map {
          case NamedJoinExpression(outputName, veType, joinExpression) =>
            joinExpression.fold(ce => ce) match {
              case ex => ex.isNotNullCode match {
                case None => CodeLines.from(
                  s"${outputName}->data[i] = ${ex.cCode};",
                  s"set_validity($outputName->validityBuffer, i, 1);"
                ).indented
                case Some(nullCheck) => CodeLines.from(
                  s"if( ${nullCheck} ) {",
                  s"${outputName}->data[i] = ${ex.cCode};",
                  s"set_validity($outputName->validityBuffer, i, 1);",
                  "} else {",
                  s"set_validity($outputName->validityBuffer, i, 0);",
                  "}"
                ).indented
              }
            }
        },
        "}",
        veInnerJoin.outputs.map{
          case NamedJoinExpression(outputName, veType, joinExpression) =>
            CodeLines.from(
              s"${outputName}->count = left_out.size();"
            )
        }

    ))
  }

  def renderOuterJoin(
                       veOuterJoin: VeOuterJoin[CVector, TypedCExpression2, TypedCExpression2, NamedJoinExpression]
                     ): CFunction = {

    CFunction(
      inputs = veOuterJoin.inputs,
      outputs = veOuterJoin.outputs.zipWithIndex.map {
        case (OuterJoinOutput(NamedJoinExpression(outputName, veType, _), _), idx) =>
          CVector(outputName, veType)
      },
      body = CodeLines.from(
        s"std::vector <std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>> left_vec;",
        "std::vector<size_t> left_idx;",
        s"std::vector <std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>> right_vec;",
        "std::vector<size_t> right_idx;",
        "#pragma _NEC ivdep",
        CodeLines.from(
          veOuterJoin.leftKey.cExpression.isNotNullCode match {
            case Some(notNullCode) => CodeLines.from(
              s"""for(int i =0; i < ${veOuterJoin.leftKey.cExpression.cCode.replace("data[i]","count")}; i++) {""",
              "left_idx.push_back(i);",
              s"if( ${notNullCode}) {",
              s"left_vec.push_back(std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>(${veOuterJoin.leftKey.cExpression.cCode}, 1));",
              "} else {",
              s"left_vec.push_back(std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>(${veOuterJoin.leftKey.cExpression.cCode}, 0));",
              "}",
              "}"
            )
            case None => CodeLines.from(
              s"""for(int i =0; i < ${veOuterJoin.leftKey.cExpression.cCode.replace("data[i]","count")}; i++) {""",
              s"left_vec.push_back(std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>(${veOuterJoin.leftKey.cExpression.cCode}, 1));",
              "}"
            )
          },
          veOuterJoin.rightKey.cExpression.isNotNullCode match {
            case Some(notNullCode) => CodeLines.from(
              s"""for(int i =0; i < ${veOuterJoin.rightKey.cExpression.cCode.replace("data[i]","count")}; i++) {""",
              "right_idx.push_back(i);",

              s"if( ${notNullCode}) {",
              s"right_vec.push_back(std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>(${veOuterJoin.rightKey.cExpression.cCode}, 1));",
              "} else {",
              s"right_vec.push_back(std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>(${veOuterJoin.rightKey.cExpression.cCode}, 0));",
              "}",
              "}"
            )
            case None => CodeLines.from(
              s"""for(int i =0; i < ${veOuterJoin.rightKey.cExpression.cCode.replace("data[i]","count")}; i++) {""",
              s"right_vec.push_back(std::tuple<${veOuterJoin.rightKey.veType.cScalarType}, int>(${veOuterJoin.rightKey.cExpression.cCode}, 1));",
              "}"
            )
          },
        ).indented,
        "std::vector<size_t> right_out;",
        "std::vector<size_t> left_out;",
        veOuterJoin.joinType match {
          case LeftOuterJoin => CodeLines.from(
            s"std::vector<size_t> outer_idx = frovedis::outer_equi_join<std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>>(left_vec, left_idx, right_vec, right_idx, left_out, right_out);",
          )
          case RightOuterJoin => CodeLines.from(
            s"std::vector<size_t> outer_idx = frovedis::outer_equi_join<std::tuple<${veOuterJoin.leftKey.veType.cScalarType}, int>>(right_vec, right_idx, left_vec, left_idx, right_out, left_out);",
          )
        },
        List("long validityBuffSize = ceil((left_out.size() + outer_idx.size()) / 8.0);"),
        veOuterJoin.outputs.map {
          case OuterJoinOutput(NamedJoinExpression(outputName, veType, joinExpression), _) =>
            joinExpression.fold(
              whenProj = _ => CodeLines.from(
                s"${outputName}->data = (${veType.cScalarType}*) malloc((left_out.size() + outer_idx.size()) * sizeof(${veType.cScalarType}));",
                s"${outputName}->validityBuffer = (unsigned char *) malloc(validityBuffSize * sizeof(unsigned char*));"
              ))
        },

        "for(int i = 0; i < left_out.size(); i++) { ",
        veOuterJoin.outputs.map {
          case OuterJoinOutput(NamedJoinExpression(outputName, veType, joinExpression), _) =>
            joinExpression.fold(ce => ce) match {
              case ex => ex.isNotNullCode match {
                case None => CodeLines.from(
                  s"${outputName}->data[i] = ${ex.cCode};",
                  s"set_validity($outputName->validityBuffer, i, 1);"
                ).indented
                case Some(nullCheck) => CodeLines.from(
                  s"if( ${nullCheck} ) {",
                  s"${outputName}->data[i] = ${ex.cCode};",
                  s"set_validity($outputName->validityBuffer, i, 1);",
                  "} else {",
                  s"set_validity($outputName->validityBuffer, i, 0);",
                  "}"
                ).indented
              }
            }
        },
        "}",
        CodeLines.from(
          "#pragma _NEC ivdep",
          "for (int i = left_out.size(); i < (left_out.size() + outer_idx.size()); i++) {",
          "int idx = i - left_out.size();"
        ),
        veOuterJoin.outputs.map{
          case OuterJoinOutput(_, NamedJoinExpression(outputName, veType, joinExpression)) => {
            joinExpression.fold(ce => ce) match {
              case ex => ex.isNotNullCode match {
                case None => CodeLines.from(
                  s"${outputName}->data[i] = ${ex.cCode};",
                  s"set_validity($outputName->validityBuffer, i, 1);"
                ).indented
                case Some(nullCheck) => CodeLines.from(
                  s"if( ${nullCheck} ) {",
                  s"${outputName}->data[i] = ${ex.cCode};",
                  s"set_validity($outputName->validityBuffer, i, 1);",
                  "} else {",
                  s"set_validity($outputName->validityBuffer, i, 0);",
                  "}"
                ).indented
              }
            }
          }

        },
        CodeLines.from("}"),
        veOuterJoin.outputs.map{
          case OuterJoinOutput(NamedJoinExpression(outputName, veType, joinExpression), _) =>
            CodeLines.from(
              s"${outputName}->count = left_out.size() + outer_idx.size();"
            )
        },
      ))
  }

  def renderGroupBy(
    veDataTransformation: VeGroupBy[CVector, TypedCExpression2, NamedGroupByExpression]
  ): CFunction = {
    val tuple =
      s"std::tuple<${veDataTransformation.groups
        .flatMap { v =>
          List(v.veType.cScalarType) ++ v.cExpression.isNotNullCode.map(_ => "int").toList
        }
        .mkString(", ")}>";
    CFunction(
      inputs = veDataTransformation.inputs,
      outputs = veDataTransformation.outputs.zipWithIndex.map {
        case (NamedGroupByExpression(outputName, veType, _), idx) =>
          CVector(outputName, veType)
      },
      body = CodeLines.from(
        s"/** sorting section - ${GroupBeforeSort} **/",
        s"std::vector<${tuple}> full_grouping_vec;",
        s"std::vector<size_t> sorted_idx(input_0->count);",
        "for ( long i = 0; i < input_0->count; i++ ) {",
        CodeLines
          .from(
            "sorted_idx[i] = i;",
            s"full_grouping_vec.push_back(${tuple}(${veDataTransformation.groups
              .flatMap { g => List(g.cExpression.cCode) ++ g.cExpression.isNotNullCode.toList }
              .mkString(", ")}));"
          )
          .indented,
        s"}",
        "frovedis::insertion_sort(full_grouping_vec.data(), sorted_idx.data(), full_grouping_vec.size());",
        "/** compute each group's range **/",
        "std::vector<size_t> groups_indices = frovedis::set_separate(full_grouping_vec);",
        s"int groups_count = groups_indices.size() - 1;",
        "/** perform computations for every output **/",
        veDataTransformation.outputs.zipWithIndex.map {
          case (NamedGroupByExpression(outputName, veType, groupByExpr), idx) =>
            CodeLines.from(
              "",
              s"// Output #$idx for ${outputName}:",
              s"$outputName->count = groups_count;",
              s"$outputName->data = (${veType.cScalarType}*) malloc($outputName->count * sizeof(${veType.cScalarType}));",
              s"$outputName->validityBuffer = (unsigned char *) malloc(ceil(groups_count / 8.0));",
              "",
              "// for each group",
              "for (size_t g = 0; g < groups_count; g++) {",
              CodeLines
                .from(
                  "// compute an aggregate",
                  groupByExpr.fold(
                    whenProj = _ => CodeLines.empty,
                    whenAgg = agg => agg.initial(outputName)
                  ),
                  "size_t group_start_in_idx = groups_indices[g];",
                  "size_t group_end_in_idx = groups_indices[g + 1];",
                  "int i = 0;",
                  s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
                  CodeLines
                    .from(
                      "i = sorted_idx[j];",
                      groupByExpr
                        .fold(whenProj = _ => CodeLines.empty, whenAgg = _.iterate(outputName))
                    )
                    .indented,
                  "}",
                  groupByExpr.fold(_ => CodeLines.empty, whenAgg = _.compute(outputName)),
                  "// store the result",
                  groupByExpr.fold(whenProj = ce => ce, whenAgg = _.fetch(outputName)) match {
                    case ex =>
                      ex.isNotNullCode match {
                        case None =>
                          CodeLines.from(
                            s"""$outputName->data[g] = ${ex.cCode};""",
                            s"set_validity($outputName->validityBuffer, g, 1);"
                          )
                        case Some(notNullCheck) =>
                          CodeLines.from(
                            s"if ( $notNullCheck ) {",
                            s"""  $outputName->data[g] = ${ex.cCode};""",
                            s"  set_validity($outputName->validityBuffer, g, 1);",
                            "} else {",
                            s"  set_validity($outputName->validityBuffer, g, 0);",
                            "}"
                          )
                      }
                  },
                  groupByExpr.fold(_ => CodeLines.empty, _.free(outputName))
                )
                .indented,
              "}"
            )
        }
      )
    )
  }

}
