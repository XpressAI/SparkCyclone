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
  TEST_SUITE("cyclone::join") {
    TEST_CASE("joining works on fully matched pairs") {
      std::vector<size_t> left = {1, 2, 3, 4, 5, 6};
      std::vector<size_t> right = {6, 3, 2, 4, 5, 1};
      std::vector<size_t> out_left;
      std::vector<size_t> out_right;

      std::vector<size_t> expected_left = {0, 1, 2, 3, 4, 5};
      std::vector<size_t> expected_right = {5, 2, 1, 3, 4, 0};

      cyclone::join::equi_join_indices(left, right, out_left, out_right);

      cyclone::io::print_vec("out_left", out_left);
      cyclone::io::print_vec("exp_left", expected_left);
      cyclone::io::print_vec("out_right", out_right);
      cyclone::io::print_vec("exp_right", expected_right);

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

      cyclone::join::equi_join_indices(left, right, out_left, out_right);

      cyclone::io::print_vec("out_left", out_left);
      cyclone::io::print_vec("exp_left", expected_left);
      cyclone::io::print_vec("out_right", out_right);
      cyclone::io::print_vec("exp_right", expected_right);

      CHECK(out_left.size() == 2);
      CHECK(out_right.size() == 2);
      CHECK(out_left == expected_left);
      CHECK(out_right == expected_right);
    }
  }
}
