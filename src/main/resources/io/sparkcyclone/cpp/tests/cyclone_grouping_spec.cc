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
#include <stddef.h>

namespace cyclone::tests {
  TEST_SUITE("cyclone::grouping") {
    TEST_CASE_TEMPLATE("sort_and_group_multiple<T, true>() works for T=", T, int32_t, int64_t, float, double) {
      std::vector<T>            input     {{ 23, 0, 1, 4, 3, -2, 1, 5, 3, 0, 1, 6, 9, 6, 42, -100 }};
      const std::vector<size_t> grouping  {{ 1, 6, 11, 14 }};
      std::vector<size_t>       index(input.size());
      for (auto i = 0; i < index.size(); i++) index[i] = i;

      const std::vector<T>      expected_input    {{ 23, -2, 0, 1, 3, 4, 0, 1, 1, 3, 5, 6, 6, 9, 42, -100 }};
      const std::vector<size_t> expected_index    {{ 0, 5, 1, 2, 4, 3, 9, 6, 10, 8, 7, 11, 13, 12, 14, 15 }};
      const std::vector<size_t> expected_grouping {{ 0, 2, 3, 4, 5, 6, 7, 9, 10, 11, 13, 14 }};

      auto new_grouping = cyclone::grouping::sort_and_group_multiple<T, true>(input, index, grouping);

      CHECK(input == expected_input);
      CHECK(index == expected_index);
      CHECK(new_grouping == expected_grouping);
    }

    TEST_CASE("separate_to_groups() works [0]") {
      std::vector<size_t> grouping = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
      std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
      std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
      std::vector<size_t> expected_keys = {0, 1};
      std::vector<size_t> keys;
      std::vector<std::vector<size_t>> groups = cyclone::grouping::separate_to_groups(grouping, keys);

      cyclone::print_vec("groups[0]", groups[0]);
      cyclone::print_vec("expect_0:", expected_0);
      cyclone::print_vec("groups[1]", groups[1]);
      cyclone::print_vec("expect_1:", expected_1);

      CHECK(groups[0] == expected_0);
      CHECK(groups[1] == expected_1);
      CHECK(keys == expected_keys);
    }

    TEST_CASE("separate_to_groups() works [1]") {
      std::vector<size_t> grouping = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
      std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
      std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
      std::vector<size_t> expected_keys = {10, 11};
      std::vector<size_t> keys;
      std::vector<std::vector<size_t>> groups = cyclone::grouping::separate_to_groups(grouping, keys);

      cyclone::print_vec("groups[0]", groups[0]);
      cyclone::print_vec("expect_0:", expected_0);
      cyclone::print_vec("groups[1]", groups[1]);
      cyclone::print_vec("expect_1:", expected_1);

      CHECK(groups[0] == expected_0);
      CHECK(groups[1] == expected_1);
      CHECK(keys == expected_keys);
    }

    TEST_CASE("separate_to_groups() works for empty grouping and keys") {
      std::vector<size_t> grouping = {};
      std::vector<size_t> keys;
      std::vector<size_t> empty_keys = {};
      std::vector<std::vector<size_t>> groups = cyclone::grouping::separate_to_groups(grouping, keys);

      CHECK(groups.size() == 0);
      CHECK(keys == empty_keys);
    }
  }
}
