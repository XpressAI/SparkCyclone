package com.nec.cmake

import com.nec.cmake.CMakeBuilder.IncludeFile
import org.scalatest.freespec.AnyFreeSpec

final class CMakeBuilderTest extends AnyFreeSpec {
  "Include file can be parsed" in {
    assert(
      IncludeFile
        .unapply("""#include "frovedis/core/radix_sort.hpp"\r""")
        .contains("frovedis/core/radix_sort.hpp")
    )
  }
}
