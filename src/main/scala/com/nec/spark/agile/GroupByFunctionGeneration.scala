package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.GroupByExpression.{
  GroupByAggregation,
  GroupByProjection
}
import com.nec.spark.agile.CFunctionGeneration.{
  CFunction,
  CScalarVector,
  CVarChar,
  CVector,
  GroupBeforeSort,
  GroupByExpression,
  NamedGroupByExpression,
  NamedStringProducer,
  StringGrouping,
  TypedCExpression2,
  VeGroupBy
}

//noinspection MapFlatten
final case class GroupByFunctionGeneration(
  veDataTransformation: VeGroupBy[
    CVector,
    Either[StringGrouping, TypedCExpression2],
    Either[NamedStringProducer, NamedGroupByExpression]
  ]
) {

  def tuple =
    s"std::tuple<${veDataTransformation.groups
      .flatMap {
        case Right(v) =>
          List(v.veType.cScalarType) ++ v.cExpression.isNotNullCode.map(_ => "int").toList
        case Left(s) =>
          List("long")
      }
      .mkString(", ")}>"

  /**
   * We return: grouping constituents followed by the aggregate partials
   */
  private def partialOutputs
    : List[Either[(NamedStringProducer, CVector), (NamedGroupByExpression, List[CVector])]] = {
    veDataTransformation.outputs.zipWithIndex.map {
      case (
            Right(
              n @ NamedGroupByExpression(outputName, veType, GroupByExpression.GroupByProjection(_))
            ),
            _
          ) =>
        Right(n -> List(CScalarVector(outputName, veType)))
      case (
            Right(
              n @ NamedGroupByExpression(
                outputName,
                veType,
                GroupByExpression.GroupByAggregation(agg)
              )
            ),
            idx
          ) =>
        Right(n -> agg.partialValues(outputName).map(_._1))
      case (e @ Left(n @ NamedStringProducer(outputName, _)), idx) =>
        Left(n -> CVarChar(outputName))
    }
  }

  private def partialOutputVectors: List[CVector] =
    partialOutputs
      .map(_.left.map(_._2).left.map(List.apply(_)))
      .map(_.right.map(_._2))
      .flatMap(_.fold(identity, identity))

  def renderPartialGroupBy: CFunction = {
    CFunction(
      inputs = veDataTransformation.inputs,
      outputs = partialOutputVectors,
      body = CodeLines.from(
        CodeLines.debugHere,
        CodeLines.from(
          s"/** sorting section - ${GroupBeforeSort} **/",
          s"std::vector<${tuple}> full_grouping_vec;",
          s"std::vector<size_t> sorted_idx(input_0->count);",
          veDataTransformation.groups.collect { case Left(StringGrouping(name)) =>
            val stringIdToHash = s"${name}_string_id_to_hash"
            CodeLines.from(
              s"std::vector<long> $stringIdToHash(input_0->count);",
              "for ( long i = 0; i < input_0->count; i++ ) {",
              CodeLines
                .from(
                  s"long string_hash = 0;",
                  s"for ( int q = ${name}->offsets[i]; q < ${name}->offsets[i + 1]; q++ ) {",
                  CodeLines.from(s"string_hash = 31*string_hash + ${name}->data[q];").indented,
                  "}",
                  s"$stringIdToHash[i] = string_hash;"
                )
                .indented,
              "}"
            )
          },
          CodeLines.debugHere,
          "for ( long i = 0; i < input_0->count; i++ ) {",
          CodeLines
            .from(
              "sorted_idx[i] = i;",
              s"full_grouping_vec.push_back(${tuple}(${{
                veDataTransformation.groups.collect { case Left(StringGrouping(name)) =>
                  List(s"${name}_string_id_to_hash[i]")
                } ++ partialOutputs
                  .collect { case Right((NamedGroupByExpression(name, veType, GroupByProjection(cExpression)), cVector :: Nil)) =>
                    List(s"${name}->data[i]", s"get_validity(${name}->validityBuffer, i)")
                  }
              }.flatten
                .mkString(", ")}));"
            )
            .indented,
          s"}",
          CodeLines.debugHere,
          "frovedis::insertion_sort(full_grouping_vec.data(), sorted_idx.data(), full_grouping_vec.size());",
          "/** compute each group's range **/",
          "std::vector<size_t> groups_indices = frovedis::set_separate(full_grouping_vec);",
          s"int groups_count = groups_indices.size() - 1;"
        ),
        "/** perform computations for every output **/",
        CodeLines.debugHere,
        CodeLines.from(
          partialOutputs
            .flatMap(_.left.toSeq)
            .map(_._1)
            .map { case NamedStringProducer(name, stringProducer) =>
              val fp = StringProducer.FilteringProducer(name, stringProducer)
              CodeLines
                .from(
                  fp.setup,
                  "// for each group",
                  "for (size_t g = 0; g < groups_count; g++) {",
                  CodeLines
                    .from("long i = sorted_idx[groups_indices[g]];", "long o = g;", fp.forEach)
                    .indented,
                  "}",
                  fp.complete,
                  "for (size_t g = 0; g < groups_count; g++) {",
                  CodeLines
                    .from(
                      "long i = sorted_idx[groups_indices[g]];",
                      "long o = g;",
                      fp.validityForEach
                    )
                    .indented,
                  "}"
                )
                .blockCommented(s"Produce the string group")
            }
        ),
        CodeLines.debugHere,
        CodeLines.from(partialOutputs.flatMap(_.right.toSeq.map(_._1)).map {
          case NamedGroupByExpression(outputName, veType, GroupByProjection(ex)) =>
            CodeLines.from(
              "",
              s"// Output ${outputName}:",
              s"$outputName->count = groups_count;",
              s"$outputName->data = (${veType.cScalarType}*) malloc($outputName->count * sizeof(${veType.cScalarType}));",
              s"$outputName->validityBuffer = (unsigned char *) malloc(ceil(groups_count / 8.0));",
              "",
              "// for each group",
              "for (size_t g = 0; g < groups_count; g++) {",
              CodeLines
                .from(
                  "// compute an aggregate",
                  "size_t group_start_in_idx = groups_indices[g];",
                  "size_t group_end_in_idx = groups_indices[g + 1];",
                  "int i = 0;",
                  s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
                  CodeLines
                    .from("i = sorted_idx[j];")
                    .indented,
                  "}",
                  "// store the result",
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
                )
                .indented,
              "}"
            )
          case NamedGroupByExpression(finalOutputName, veType, GroupByAggregation(agg)) =>
            CodeLines.from(
              "",
              s"// Partials' output for ${finalOutputName}:",
              agg.partialValues(s"${finalOutputName}").map {
                case (CScalarVector(outputName, partialType), cExpression) =>
                  CodeLines.from(
                    s"$outputName->count = groups_count;",
                    s"$outputName->data = (${partialType.cScalarType}*) malloc($outputName->count * sizeof(${partialType.cScalarType}));",
                    s"$outputName->validityBuffer = (unsigned char *) malloc(ceil(groups_count / 8.0));"
                  )
              },
              "",
              "// for each group",
              "for (size_t g = 0; g < groups_count; g++) {",
              CodeLines
                .from(
                  "// compute an aggregate",
                  agg.initial(finalOutputName),
                  "size_t group_start_in_idx = groups_indices[g];",
                  "size_t group_end_in_idx = groups_indices[g + 1];",
                  "int i = 0;",
                  s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
                  CodeLines
                    .from("i = sorted_idx[j];", agg.iterate(finalOutputName))
                    .indented,
                  "}",
                  agg.compute(finalOutputName),
                  "// store the result",
                  agg.partialValues(s"${finalOutputName}").map {
                    case (CScalarVector(outputName, partialType), cExpression) =>
                      CodeLines.from(
                        s"$outputName->data[g] = ${cExpression.cCode};",
                        s"set_validity($outputName->validityBuffer, g, 1);"
                      )
                  }
                )
                .indented,
              "}"
            )
        })
      )
    )
  }

  private def partialInputs
    : List[Either[(NamedStringProducer, CVector), (NamedGroupByExpression, List[CVector])]] =
    partialOutputs.map {
      case Left((nsp, cv)) => Left((nsp, cv.replaceName("output", "input")))
      case Right((ng, lv)) => Right((ng, lv.map(_.replaceName("output", "input"))))
    }

  def partialInputVectors: List[CVector] =
    partialOutputVectors.map(_.replaceName("output", "input"))

  def renderFinalGroupBy: CFunction = {
    CFunction(
      inputs = partialInputVectors,
      outputs = renderGroupBy.outputs,
      body = CodeLines.from(
        CodeLines.from(
          s"/** sorting section - ${GroupBeforeSort} **/",
          s"std::vector<${tuple}> full_grouping_vec;",
          s"std::vector<size_t> sorted_idx(input_0->count);",
          CodeLines.debugHere,
          veDataTransformation.groups.collect { case Left(StringGrouping(name)) =>
            val stringIdToHash = s"${name}_string_id_to_hash"
            CodeLines.from(
              s"std::vector<long> $stringIdToHash(input_0->count);",
              "for ( long i = 0; i < input_0->count; i++ ) {",
              CodeLines
                .from(
                  s"long string_hash = 0;",
                  s"for ( int q = ${name}->offsets[i]; q < ${name}->offsets[i + 1]; q++ ) {",
                  CodeLines.from(s"string_hash = 31*string_hash + ${name}->data[q];").indented,
                  "}",
                  s"$stringIdToHash[i] = string_hash;"
                )
                .indented,
              "}"
            )
          },
          CodeLines.debugHere,
          "for ( long i = 0; i < input_0->count; i++ ) {",
          CodeLines
            .from(
              "sorted_idx[i] = i;",
              s"full_grouping_vec.push_back(${tuple}(${veDataTransformation.groups
                .flatMap {
                  case Right(g) => List(g.cExpression.cCode) ++ g.cExpression.isNotNullCode.toList
                  case Left(StringGrouping(inputName)) =>
                    List(s"${inputName}_string_id_to_hash[i]")
                }
                .mkString(", ")}));"
            )
            .indented,
          s"}",
          "frovedis::insertion_sort(full_grouping_vec.data(), sorted_idx.data(), full_grouping_vec.size());",
          "/** compute each group's range **/",
          "std::vector<size_t> groups_indices = frovedis::set_separate(full_grouping_vec);",
          s"int groups_count = groups_indices.size() - 1;"
        ),
        CodeLines.debugHere,
        "/** perform computations for every output **/",
        CodeLines.from(
          partialInputs
            .flatMap(_.left.toSeq)
            .map(_._1)
            .map { case NamedStringProducer(name, stringProducer) =>
              val fp = StringProducer.FilteringProducer(name, stringProducer)
              CodeLines
                .from(
                  fp.setup,
                  "// for each group",
                  CodeLines.debugHere,
                  "for (size_t g = 0; g < groups_count; g++) {",
                  CodeLines
                    .from("long i = sorted_idx[groups_indices[g]];", "long o = g;", fp.forEach)
                    .indented,
                  "}",
                  fp.complete,
                  "for (size_t g = 0; g < groups_count; g++) {",
                  CodeLines
                    .from(
                      "long i = sorted_idx[groups_indices[g]];",
                      "long o = g;",
                      fp.validityForEach
                    )
                    .indented,
                  "}"
                )
                .blockCommented(s"Produce the string group")
            }
        ),
        CodeLines.from(partialInputs.flatMap(_.right.toSeq).map {
          case (NamedGroupByExpression(outputName, veType, GroupByAggregation(agg)), vecs) =>
            CodeLines.from(
              CodeLines.debugHere,
              s"// Output for ${outputName}:",
              s"$outputName->count = groups_count;",
              s"$outputName->data = (${veType.cScalarType}*) malloc($outputName->count * sizeof(${veType.cScalarType}));",
              s"$outputName->validityBuffer = (unsigned char *) malloc(ceil(groups_count / 8.0));",
              "",
              "// for each group",
              "for (size_t g = 0; g < groups_count; g++) {",
              CodeLines
                .from(
                  agg.initial(outputName),
                  "size_t group_start_in_idx = groups_indices[g];",
                  "size_t group_end_in_idx = groups_indices[g + 1];",
                  "int i = 0;",
                  s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
                  CodeLines
                    .from(
                      "i = sorted_idx[j];",
                      "// merge partial results",
                      agg.merge(outputName, outputName.replaceAllLiterally("output", "input"))
                    )
                    .indented,
                  "}",
                  agg.compute(outputName),
                  "// store the result",
                  agg.fetch(outputName) match {
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
                  agg.free(outputName)
                )
                .indented,
              "}"
            )

          case (NamedGroupByExpression(outputName, veType, GroupByProjection(ex)), _) =>
            CodeLines.from(
              CodeLines.debugHere,
              "",
              s"// Output ${outputName}:",
              s"$outputName->count = groups_count;",
              s"$outputName->data = (${veType.cScalarType}*) malloc($outputName->count * sizeof(${veType.cScalarType}));",
              s"$outputName->validityBuffer = (unsigned char *) malloc(ceil(groups_count / 8.0));",
              "",
              "// for each group",
              "for (size_t g = 0; g < groups_count; g++) {",
              CodeLines
                .from(
                  "// compute an aggregate",
                  "size_t group_start_in_idx = groups_indices[g];",
                  "size_t group_end_in_idx = groups_indices[g + 1];",
                  "int i = 0;",
                  s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
                  CodeLines
                    .from("i = sorted_idx[j];")
                    .indented,
                  "}",
                  "// store the result",
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
                )
                .indented,
              "}"
            )
        })
      )
    )
  }

  def renderGroupBy: CFunction = {
    CFunction(
      inputs = veDataTransformation.inputs,
      outputs = veDataTransformation.outputs.zipWithIndex.map {
        case (Right(NamedGroupByExpression(outputName, veType, _)), idx) =>
          CScalarVector(outputName, veType)
        case (Left(NamedStringProducer(outputName, _)), idx) =>
          CVarChar(outputName)
      },
      body = CodeLines.from(
        CodeLines.from(
          s"/** sorting section - ${GroupBeforeSort} **/",
          s"std::vector<${tuple}> full_grouping_vec;",
          s"std::vector<size_t> sorted_idx(input_0->count);",
          veDataTransformation.groups.collect { case Left(StringGrouping(name)) =>
            val stringIdToHash = s"${name}_string_id_to_hash"
            CodeLines.from(
              s"std::vector<long> $stringIdToHash(input_0->count);",
              "for ( long i = 0; i < input_0->count; i++ ) {",
              CodeLines
                .from(
                  // todo replace with a proper hash. I cannot do this quickly in C.
                  s"long string_hash = 0;",
                  s"for ( int q = ${name}->offsets[i]; q < ${name}->offsets[i + 1]; q++ ) {",
                  CodeLines.from(s"string_hash = 31*string_hash + ${name}->data[q];").indented,
                  "}",
                  s"$stringIdToHash[i] = string_hash;"
                )
                .indented,
              "}"
            )
          },
          "for ( long i = 0; i < input_0->count; i++ ) {",
          CodeLines
            .from(
              "sorted_idx[i] = i;",
              s"full_grouping_vec.push_back(${tuple}(${veDataTransformation.groups
                .flatMap {
                  case Right(g) => List(g.cExpression.cCode) ++ g.cExpression.isNotNullCode.toList
                  case Left(StringGrouping(inputName)) =>
                    List(s"${inputName}_string_id_to_hash[i]")
                }
                .mkString(", ")}));"
            )
            .indented,
          s"}",
          "frovedis::insertion_sort(full_grouping_vec.data(), sorted_idx.data(), full_grouping_vec.size());",
          "/** compute each group's range **/",
          "std::vector<size_t> groups_indices = frovedis::set_separate(full_grouping_vec);",
          s"int groups_count = groups_indices.size() - 1;"
        ),
        "/** perform computations for every output **/",
        veDataTransformation.outputs.zipWithIndex.map {
          case (Left(NamedStringProducer(name, stringProducer)), idx) =>
            val fp = StringProducer.FilteringProducer(name, stringProducer)
            CodeLines
              .from(
                fp.setup,
                "// for each group",
                "for (size_t g = 0; g < groups_count; g++) {",
                CodeLines
                  .from("long i = sorted_idx[groups_indices[g]];", "long o = g;", fp.forEach)
                  .indented,
                "}",
                fp.complete,
                "for (size_t g = 0; g < groups_count; g++) {",
                CodeLines
                  .from(
                    "long i = sorted_idx[groups_indices[g]];",
                    "long o = g;",
                    fp.validityForEach
                  )
                  .indented,
                "}"
              )
              .blockCommented(s"Produce the string group")
          case (Right(NamedGroupByExpression(outputName, veType, groupByExpr)), idx) =>
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
