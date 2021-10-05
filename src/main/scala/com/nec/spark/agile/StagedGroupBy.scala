package com.nec.spark.agile

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{
  CExpression,
  CFunction,
  CVector,
  VeScalarType,
  VeType
}
import com.nec.spark.agile.StagedGroupBy.{
  GroupingCodeGenerator,
  GroupingKey,
  StagedAggregation,
  StagedProjection
}

/**
 * In a Staged groupBy, in the first function:
 * 1. Perform the necessary computations of grouping keys (eg. group by f(x), g(y), h(z)
 * 2. Perform the computation of the groups
 * 3. Perform the projections of eg select f(x) group by x
 * 4. Perform the partial aggregations eg select avg(x) ==> x_sum, x_count
 * 5. Return the grouping keys + projections + aggregations
 *
 * In the second function:
 * 1. Perform the computation of the groups (grouping keys have already been provides)
 * 2. Perform the merge of aggregations, final_x_sum += x_sum; final_x_count += x_count (per each group)
 * 3. Compute the final aggregation result eg final_avg = final_x_sum / final_x_count
 * 4. Return the projections + aggregations in the intended return order
 *
 * @param groupingKeys Things to group by -- there may be things we group by, but are not part of finalOutputs, hence
 *                     the below data structure and finalOutputs
 * @param finalOutputs Ordered final outputs that we will give back to Spark
 */
final case class StagedGroupBy(
  groupingKeys: List[GroupingKey],
  finalOutputs: List[Either[GroupingKey, Either[StagedProjection, StagedAggregation]]]
) {

  def gcg: GroupingCodeGenerator = ???

  def intermediateTypes: List[VeType] =
    groupingKeys
      .map(_.veType) ++ finalOutputs
      .flatMap(_.right.toSeq)
      .flatMap(_.left.toSeq)
      .map(_.veType) ++ finalOutputs
      .flatMap(_.right.toSeq)
      .flatMap(_.right.toSeq)
      .flatMap(_.attributes.map(_.veType))

  def outputs: List[CVector] = finalOutputs.map {
    case Left(groupingKey)             => groupingKey.veType.makeCVector(groupingKey.name)
    case Right(Left(stagedProjection)) => stagedProjection.veType.makeCVector(stagedProjection.name)
    case Right(Right(stagedAggregation)) =>
      stagedAggregation.finalType.makeCVector(stagedAggregation.name)
  }

  def projections: List[StagedProjection] =
    finalOutputs.flatMap(_.right.toSeq).flatMap(_.left.toSeq)

  def aggregations: List[StagedAggregation] =
    finalOutputs.flatMap(_.right.toSeq).flatMap(_.right.toSeq)

  private def partials: List[CVector] = {
    List(
      groupingKeys.map(gk => gk.veType.makeCVector(gk.name)),
      projections.map(pr => pr.veType.makeCVector(pr.name)),
      aggregations.flatMap(agg => agg.attributes.map(att => att.veType.makeCVector(att.name)))
    ).flatten
  }

  def computeGroupingKeys: CodeLines = ???

  def computeAggregatePartialsPerGroup: CodeLines = ???

  def computeGroupingKeysPerGroup: CodeLines = ???

  def computeProjectionsPerGroup(compute: StagedProjection => Option[CExpression]): CodeLines = {
    CodeLines.from(projections.map { case sp @ StagedProjection(name, veType: VeScalarType) =>
      compute(sp) match {
        case None => sys.error(s"Could not map ${sp}")
        case Some(cExpression) =>
          CodeLines.from(
            StagedGroupBy.initializeOutputVector(veType, name, gcg.groupsCountOutName),
            gcg.forHeadOfEachGroup(StagedGroupBy.storeTo(name, cExpression, "g"))
          )
      }
    })
  }

  def createPartial(computeProjection: StagedProjection => Option[CExpression]): CFunction =
    CFunction(
      inputs = ???,
      outputs = partials,
      body = {
        CodeLines.from(
          computeGroupingKeys,
          performGrouping,
          computeGroupingKeysPerGroup,
          computeProjectionsPerGroup(computeProjection),
          computeAggregatePartialsPerGroup
        )
      }
    )

  def performGrouping: CodeLines = ???

  def mergeAndProduceAggregatePartialsPerGroup: CodeLines = ???

  def passProjectionsPerGroup: CodeLines = ???

  def passGroupingKeysPerGroup: CodeLines = ???

  def createFinal: CFunction = CFunction(
    inputs = partials,
    outputs = outputs,
    body = {
      CodeLines.from(
        performGrouping,
        mergeAndProduceAggregatePartialsPerGroup,
        passProjectionsPerGroup,
        passGroupingKeysPerGroup
      )
    }
  )

}

object StagedGroupBy {
  final case class GroupingKey(name: String, veType: VeType)
  final case class StagedProjection(name: String, veType: VeType)
  final case class StagedAggregationAttribute(name: String, veType: VeType)
  final case class StagedAggregation(
    name: String,
    finalType: VeType,
    attributes: List[StagedAggregationAttribute]
  )

  final case class GroupingCodeGenerator(
    groupingVecName: String,
    groupsCountOutName: String,
    groupsIndicesName: String,
    sortedIdxName: String
  ) {

    def identifyGroups(
      tupleType: String,
      count: String,
      thingsToGroup: List[Either[String, CExpression]]
    ): CodeLines = {
      val stringsToHash: List[String] = thingsToGroup.flatMap(_.left.toSeq)
      CodeLines.from(
        s"std::vector<${tupleType}> ${groupingVecName};",
        s"std::vector<size_t> ${sortedIdxName}(${count});",
        stringsToHash.map { name =>
          val stringIdToHash = s"${name}_string_id_to_hash"
          val stringHashTmp = s"${name}_string_id_to_hash_tmp"
          CodeLines.from(
            s"std::vector<long> $stringIdToHash(${count});",
            s"for ( long i = 0; i < ${count}; i++ ) {",
            CodeLines
              .from(
                s"long ${stringHashTmp} = 0;",
                s"for ( int q = ${name}->offsets[i]; q < ${name}->offsets[i + 1]; q++ ) {",
                CodeLines
                  .from(s"${stringHashTmp} = 31*${stringHashTmp} + ${name}->data[q];")
                  .indented,
                "}",
                s"$stringIdToHash[i] = ${stringHashTmp};"
              )
              .indented,
            "}"
          )
        },
        CodeLines.debugHere,
        s"for ( long i = 0; i < ${count}; i++ ) {",
        CodeLines
          .from(
            s"${sortedIdxName}[i] = i;",
            s"${groupingVecName}.push_back(${tupleType}(${thingsToGroup
              .flatMap {
                case Right(g) => List(g.cCode) ++ g.isNotNullCode.toList
                case Left(stringName) =>
                  List(s"${stringName}_string_id_to_hash[i]")
              }
              .mkString(", ")}));"
          )
          .indented,
        s"}",
        s"frovedis::insertion_sort(${groupingVecName}.data(), ${sortedIdxName}.data(), ${groupingVecName}.size());",
        "/** compute each group's range **/",
        s"std::vector<size_t> ${groupsIndicesName} = frovedis::set_separate(${groupingVecName});",
        s"int ${groupsCountOutName} = ${groupsIndicesName}.size() - 1;"
      )
    }

    def forHeadOfEachGroup(f: => CodeLines): CodeLines =
      CodeLines
        .from(
          s"for (size_t g = 0; g < ${groupsCountOutName}; g++) {",
          CodeLines
            .from(s"long i = ${sortedIdxName}[${groupsIndicesName}[g]];", f)
            .indented,
          "}"
        )

    def forEachGroupItem(
      beforeFirst: => CodeLines,
      perItem: => CodeLines,
      afterLast: => CodeLines
    ): CodeLines =
      CodeLines.from(
        s"for (size_t g = 0; g < ${groupsCountOutName}; g++) {",
        CodeLines
          .from(
            s"size_t group_start_in_idx = ${groupsIndicesName}[g];",
            s"size_t group_end_in_idx = ${groupsIndicesName}[g + 1];",
            "int i = 0;",
            beforeFirst,
            s"for ( size_t j = group_start_in_idx; j < group_end_in_idx; j++ ) {",
            CodeLines
              .from(s"i = ${sortedIdxName}[j];", perItem)
              .indented,
            "}",
            afterLast
          )
          .indented,
        "}"
      )
  }

  def storeTo(outputName: String, cExpression: CExpression, idx: String): CodeLines = {
    cExpression.isNotNullCode match {
      case None =>
        CodeLines.from(
          s"""$outputName->data[g] = ${cExpression.cCode};""",
          s"set_validity($outputName->validityBuffer, ${idx}, 1);"
        )
      case Some(notNullCheck) =>
        CodeLines.from(
          s"if ( $notNullCheck ) {",
          CodeLines
            .from(
              s"""$outputName->data[${idx}] = ${cExpression.cCode};""",
              s"set_validity($outputName->validityBuffer, ${idx}, 1);"
            )
            .indented,
          "} else {",
          CodeLines.from(s"set_validity($outputName->validityBuffer, ${idx}, 0);").indented,
          "}"
        )
    }
  }

  def initializeOutputVector(
    veScalarType: VeScalarType,
    outputName: String,
    count: String
  ): CodeLines =
    CodeLines.from(
      s"// Output for ${outputName}:",
      s"$outputName->count = ${count};",
      s"$outputName->data = (${veScalarType.cScalarType}*) malloc($outputName->count * sizeof(${veScalarType.cScalarType}));",
      s"$outputName->validityBuffer = (unsigned char *) malloc(ceil(${count} / 8.0));"
    )

}
