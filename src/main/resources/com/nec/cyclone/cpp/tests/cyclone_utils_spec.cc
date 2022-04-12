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
      //std::vector<size_t> grouping = { 0, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 1 };
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

    //cyclone::print_vec("out_left", out_left);
    //cyclone::print_vec("exp_left", expected_left);
    //cyclone::print_vec("out_right", out_right);
    //cyclone::print_vec("exp_right", expected_right);

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

  TEST_CASE("separate_to_groups_multi() short-circuit works") {
    std::vector<size_t> grouping = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
    std::vector<size_t> expected_1 = { 2, 7, 8, 9, 10, 13, 14 };
    std::vector<size_t> expected_0 = { 0, 1, 3, 4, 5, 6, 11, 12 };
    std::vector<size_t> expected_keys = {10, 11};

    std::vector<std::vector<size_t>> input(1);
    input[0] = grouping;

    std::vector<size_t> keys;
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups_multi(input, keys);
    cyclone::print_vec("groups[0]", groups[0]);
    cyclone::print_vec("expect_0:", expected_0);
    cyclone::print_vec("groups[1]", groups[1]);
    cyclone::print_vec("expect_1:", expected_1);

    CHECK(groups[0] == expected_0);
    CHECK(groups[1] == expected_1);
    CHECK(keys == expected_keys);
  }

  TEST_CASE("separate_to_groups_multi() works") {
    std::vector<size_t> grouping_1 = { 10, 10, 11, 10, 10, 10, 10, 11, 11, 11, 11, 10, 10, 11, 11 };
    std::vector<size_t> grouping_2 = { 10,  7, 11,  7,  5, 10, 10,  5, 11,  3, 11,  3, 10, 11,  7 };
    std::vector<size_t> grouping_3 = {  1,  8,  1,  8,  1,  8,  1,  8,  1,  8,  1,  8,  1,  8,  1 };

    std::vector<size_t> expected_keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    std::vector<size_t> expected_0 = { 11 };
    std::vector<size_t> expected_1 = { 4 };
    std::vector<size_t> expected_2 = { 1, 3 };
    std::vector<size_t> expected_3 = { 0, 6, 12 };
    std::vector<size_t> expected_4 = { 5 };
    std::vector<size_t> expected_5 = { 9 };
    std::vector<size_t> expected_6 = { 7 };
    std::vector<size_t> expected_7 = { 14 };
    std::vector<size_t> expected_8 = { 2, 8, 10 };
    std::vector<size_t> expected_9 = { 13 };

    std::vector<std::vector<size_t>> input(3);
    input[0] = grouping_1;
    input[1] = grouping_2;
    input[2] = grouping_3;

    std::vector<size_t> keys;
    std::vector<std::vector<size_t>> groups = cyclone::separate_to_groups_multi(input, keys);

    CHECK(keys == expected_keys);
    CHECK(groups[0] == expected_0);
    CHECK(groups[1] == expected_1);
    CHECK(groups[2] == expected_2);
    CHECK(groups[3] == expected_3);
    CHECK(groups[4] == expected_4);
    CHECK(groups[5] == expected_5);
    CHECK(groups[6] == expected_6);
    CHECK(groups[7] == expected_7);
    CHECK(groups[8] == expected_8);
    CHECK(groups[9] == expected_9);

  }
}
