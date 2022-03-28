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
#include <tuple>

namespace cyclone::tests {
  TEST_CASE("bitmask_to_matching_ids() works") {
    std::vector<size_t> bitmask = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
    std::vector<size_t> expected = { 2, 7, 8, 9, 10, 13, 14 };
    CHECK(cyclone::bitmask_to_matching_ids(bitmask) == expected);
  }

  TEST_CASE("std::vector print works") {
    std::vector<size_t> vec = { 2, 7, 8, 9, 10, 13, 14 };
    std::cout << vec << std::endl;
  }

  TEST_CASE("std::tuple print works") {
    auto tup = std::make_tuple(5, "Hello", -0.1);
    std::cout << tup << std::endl;
  }

  TEST_CASE("separate_to_groups() works") {
      std::vector<size_t> bitmask = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
      std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
      std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
      std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(bitmask);
      CHECK(groups[0] == expected_0);
      CHECK(groups[1] == expected_1);
  }
}
