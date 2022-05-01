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
      // std::vector<size_t> grouping = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
      std::vector<size_t> grouping = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
      std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
      std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
      std::vector<size_t> expected_keys = {10, 11};
      std::vector<size_t> keys;
      std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, keys);

      cyclone::print_vec("groups[0]", groups[0]);
      cyclone::print_vec("expect_0:", expected_0);
      cyclone::print_vec("groups[1]", groups[1]);
      cyclone::print_vec("expect_1:", expected_1);

      CHECK(groups[0] == expected_0);
      CHECK(groups[1] == expected_1);
      CHECK(keys == expected_keys);
  }

  TEST_CASE("old separate_to_groups() works") {
    std::vector<size_t> grouping = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
    std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
    std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
    std::vector<size_t> expected_keys = {0, 1};
    std::vector<size_t> keys;
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, keys);

    cyclone::print_vec("groups[0]", groups[0]);
    cyclone::print_vec("expect_0:", expected_0);
    cyclone::print_vec("groups[1]", groups[1]);
    cyclone::print_vec("expect_1:", expected_1);

    CHECK(groups[0] == expected_0);
    CHECK(groups[1] == expected_1);
    CHECK(keys == expected_keys);
  }

  TEST_CASE("empty separate_to_groups() is still ok.") {
    std::vector<size_t> grouping = {};
    std::vector<size_t> keys;
    std::vector<size_t> empty_keys = {};
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups(grouping, keys);

    CHECK(groups.size() == 0);
    CHECK(keys == empty_keys);
  }

  TEST_CASE("joining works on fully matched pairs") {
    std::vector<size_t> left = {1, 2, 3, 4, 5, 6};
    std::vector<size_t> right = {6, 3, 2, 4, 5, 1};
    std::vector<size_t> out_left;
    std::vector<size_t> out_right;

    std::vector<size_t> expected_left = {0, 1, 2, 3, 4, 5};
    std::vector<size_t> expected_right = {5, 2, 1, 3, 4, 0};

    cyclone::equi_join_indices(left, right, out_left, out_right);

    cyclone::print_vec("out_left", out_left);
    cyclone::print_vec("exp_left", expected_left);
    cyclone::print_vec("out_right", out_right);
    cyclone::print_vec("exp_right", expected_right);

    CHECK(out_left.size() == left.size());
    CHECK(out_right.size() == right.size());
    CHECK(out_left == expected_left);
    CHECK(out_right == expected_right);
  }

  TEST_CASE("joining works on partially unmatched pairs") {
    std::vector<size_t> left = {1, 2, 3, 4, 5, 6};
    std::vector<size_t> right = {12, 3, 1, 69, 0};
    std::vector<size_t> out_left;
    std::vector<size_t> out_right;

    std::vector<size_t> expected_left = {0, 2};
    std::vector<size_t> expected_right = {2, 1};

    cyclone::equi_join_indices(left, right, out_left, out_right);

    cyclone::print_vec("out_left", out_left);
    cyclone::print_vec("exp_left", expected_left);
    cyclone::print_vec("out_right", out_right);
    cyclone::print_vec("exp_right", expected_right);

    CHECK(out_left.size() == 2);
    CHECK(out_right.size() == 2);
    CHECK(out_left == expected_left);
    CHECK(out_right == expected_right);
  }
}
