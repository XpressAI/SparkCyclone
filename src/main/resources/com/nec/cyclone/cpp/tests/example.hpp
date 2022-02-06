#pragma once

#include "cyclone/example.hpp"
#include "tests/doctest.h"

namespace cyclone {
  TEST_CASE("Testing the factorial function") {
      CHECK(factorial(0) == 1);
      CHECK(factorial(1) == 1);
      CHECK(factorial(2) == 2);
      CHECK(factorial(3) == 6);
      CHECK(factorial(10) == 3628800);
  }
}
