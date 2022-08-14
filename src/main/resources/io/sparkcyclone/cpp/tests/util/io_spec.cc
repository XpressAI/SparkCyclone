/*
 * Copyright (c) 2022 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include "cyclone/cyclone.hpp"
#include "tests/doctest.h"

namespace cyclone::tests {
  TEST_SUITE("cyclone::io") {
    TEST_CASE_TEMPLATE("std::vector stream print works for T=", T, int32_t, int64_t, size_t, float, double) {
      auto vec = std::vector<T> {{ 2, 7, 8, 9, 10, 13, 14 }};
      std::cout << vec << std::endl;
    }

    TEST_CASE_TEMPLATE("std::vector method print works for T=", T, int32_t, int64_t, size_t, float, double) {
      auto vec = std::vector<T> {{ 2, 7, 8, 9, 10, 13, 14 }};
      cyclone::io::print_vec("example", vec);
      cyclone::io::print_vec("example", vec.data(), vec.size());
    }

    TEST_CASE("std::tuple print works") {
      auto tup = std::make_tuple(5, "Hello", -0.1);
      std::cout << tup << std::endl;
    }

    TEST_CASE("String formatting works") {
      int i = 3;
      float f = 5.f;
      char* s0 = "hello";
      std::string s1 = "world";

      auto output = cyclone::io::format("i=%d, f=%f, s=%s %s", i, f, s0, s1);
      auto expected = std::string("i=3, f=5.000000, s=hello world");

      CHECK(output == expected);
    }
  }
}
