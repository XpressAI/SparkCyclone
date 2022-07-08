package io.sparkcyclone.spark.codegen.groupby

import io.sparkcyclone.spark.codegen.CFunctionGeneration.CExpression
import io.sparkcyclone.spark.codegen.core.CodeLines

final case class GroupingCodeGenerator(
                                        groupingVecName: String,
                                        groupsCountOutName: String,
                                        groupsIndicesName: String,
                                        sortedIdxName: String
                                      ) {

  def identifyGroups(
                      tupleTypes: Seq[String],
                      tupleType: String,
                      count: String,
                      thingsToGroup: Seq[Either[String, CExpression]]
                    ): CodeLines = {
    val elems = thingsToGroup.map {
      case Left(g) =>
        g
      case Right(g) =>
        g.cCode.replace("->data[i]", "")
    }

    CodeLines.from(
      elems match {
        case Nil => CodeLines.from(
          s"size_t count = static_cast<size_t>(${count});",
          "size_t groups_count = 1;",
          "size_t* groups_indices = static_cast<size_t *>(malloc(sizeof(size_t) * 2));",
          "groups_indices[0] = 0;",
          "groups_indices[1] = count;",
          "size_t* sorted_idx = static_cast<size_t *>(malloc(sizeof(size_t) * count));",
          "#pragma _NEC vector",
          "for(auto i = 0; i < count; i++) sorted_idx[i] = i;"
        )
        case head :: Nil => CodeLines.from(
          s"size_t count = static_cast<size_t>(${count});",
          "size_t start_group[2] = {0, count};",
          "size_t* sorted_idx = static_cast<size_t *>(malloc(sizeof(size_t) * count));",
          "size_t* groups_indices = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));",
          "size_t groups_count;",
          s"${head}->group_indexes_on_subset(nullptr, start_group, 2, sorted_idx, groups_indices, groups_count);",
          "groups_count--;"
        )
        case _ => CodeLines.from(
          s"size_t count = static_cast<size_t>(${count});",
          "size_t start_group[2] = {0, count};",
          "size_t* a_idx_out = static_cast<size_t *>(malloc(sizeof(size_t) * count));",
          "size_t* a_group_out = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));",
          "size_t a_group_size_out;",
          "size_t* b_idx_out = static_cast<size_t *>(malloc(sizeof(size_t) * count));",
          "size_t* b_group_out = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));",
          "size_t b_group_size_out;",
          elems.zipWithIndex.map{ case (key, idx) =>
            val (iterArrIn, groupIn, groupSizeIn, iterArrOut, groupOut, groupSizeOut) = idx match {
              case 0 =>
                ("nullptr", "start_group", "2", "a_idx_out", "a_group_out", "a_group_size_out")
              case n if n % 2 == 1 =>
                ("a_idx_out", "a_group_out", "a_group_size_out", "b_idx_out", "b_group_out", "b_group_size_out")
              case _ =>
                ("b_idx_out", "b_group_out", "b_group_size_out", "a_idx_out", "a_group_out", "a_group_size_out")
            }

            CodeLines.from(
            s"${key}->group_indexes_on_subset($iterArrIn, $groupIn, $groupSizeIn, $iterArrOut, $groupOut, $groupSizeOut);",
          )},
          if(elems.size % 2 == 1){
            CodeLines.from(
              "free(b_idx_out);",
              "free(b_group_out);",
              "size_t* sorted_idx = a_idx_out;",
              "size_t* groups_indices = a_group_out;",
              "size_t groups_count = a_group_size_out;"
            )
          }else{
            CodeLines.from(
              "free(a_idx_out);",
              "free(a_group_out);",
              "size_t* sorted_idx = b_idx_out;",
              "size_t* groups_indices = b_group_out;",
              "size_t groups_count = b_group_size_out;"
            )
          },
          "groups_count--;"
        )
      }
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
          "#pragma cdir nodep",
          "#pragma _NEC ivdep",
          "#pragma _NEC vovertake",
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
