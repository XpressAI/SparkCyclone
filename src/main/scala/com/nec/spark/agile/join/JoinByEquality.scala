package com.nec.spark.agile.join

import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.{CScalarVector, CVarChar, CVector}
import com.nec.spark.agile.join.GenericJoiner.{
  computeNumJoin,
  computeStringJoin,
  EqualityPairing,
  Join
}

final case class JoinByEquality(
  inputsLeft: List[CVector],
  inputsRight: List[CVector],
  joins: List[Join]
) {

  def pairings: List[EqualityPairing] = joins.map {
    case Join(CVarChar(left), CVarChar(right)) =>
      EqualityPairing(
        indexOfFirstColumn = s"index_${left}",
        indexOfSecondColumn = s"index_${right}"
      )

    case Join(CScalarVector(left, _), CScalarVector(right, _)) =>
      EqualityPairing(
        indexOfFirstColumn = s"index_${left}",
        indexOfSecondColumn = s"index_${right}"
      )
    case other => sys.error(s"Unmatched => $other")
  }

  private def io: List[CVector] =
    inputsLeft ++ inputsRight ++ List(CVector.int("left_idx"), CVector.int("right_idx"))

  def produceIndices(fName: String): CodeLines = CodeLines.from(
    """#include "frovedis/core/radix_sort.hpp"""",
    """#include "frovedis/dataframe/join.hpp"""",
    """#include "frovedis/dataframe/join.cc"""",
    """#include "frovedis/text/words.hpp"""",
    """#include "frovedis/text/words.cc"""",
    """#include "frovedis/text/dict.hpp"""",
    """#include "frovedis/text/dict.cc"""",
    """#include <iostream>""",
    """#include <vector>""",
    """#include <cmath>""",
    s"""extern "C" long ${fName}(""",
    io
      .map(_.declarePointer)
      .mkString(",\n"),
    ") {",
    joins.map {
      case Join(CVarChar(left), CVarChar(right)) =>
        val pairing = EqualityPairing(
          indexOfFirstColumn = s"index_${left}",
          indexOfSecondColumn = s"index_${right}"
        )
        val left_words = s"${left}_words"
        val left_dict = s"${left}_dict"
        val left_dict_indices = s"${left}_dict_indices"
        CodeLines.from(
          CodeLines
            .from(
              s"frovedis::words ${left_words} = varchar_vector_to_words(${left});",
              s"frovedis::dict ${left_dict} = frovedis::make_dict(${left_words});"
            ),
          computeStringJoin(
            leftDictIndices = left_dict_indices,
            outMatchingIndicesLeft = pairing.indexOfFirstColumn,
            outMatchingIndicesRight = pairing.indexOfSecondColumn,
            inLeftDict = left_dict,
            inLeftWords = left_words,
            inRightVarChar = right
          )
        )

      case Join(CScalarVector(left, _), CScalarVector(right, _)) =>
        val pairing = EqualityPairing(
          indexOfFirstColumn = s"index_${left}",
          indexOfSecondColumn = s"index_${right}"
        )
        computeNumJoin(
          outMatchingIndicesLeft = pairing.indexOfFirstColumn,
          outMatchingIndicesRight = pairing.indexOfSecondColumn,
          inLeft = left,
          inRight = right
        )
      case other =>
        sys.error(s"Unmatched join: ${other}")
    },
    pairings.map { equalityPairing =>
      CodeLines.from(
        s"for ( int i = 0; i < ${equalityPairing.indexOfFirstColumn}.size(); i++ ) {",
        s"  std::cout << " + "\"" + equalityPairing.indexOfFirstColumn + " - \"" + s" << ${equalityPairing.indexOfFirstColumn}[i] << std::endl << std::flush;",
        "}",
        s"for ( int i = 0; i < ${equalityPairing.indexOfSecondColumn}.size(); i++ ) {",
        s"  std::cout << " + "\"" + equalityPairing.indexOfSecondColumn + " - \"" + s" << ${equalityPairing.indexOfSecondColumn}[i] << std::endl << std::flush;",
        "}"
      )
    },
    "return 0;",
    "}"
  )

}
