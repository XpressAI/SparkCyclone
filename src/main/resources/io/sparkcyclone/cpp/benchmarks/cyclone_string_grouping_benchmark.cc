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
#include "benchmarks/nanobench.h"
#include "tests/doctest.h"
#include "cyclone/cyclone.hpp"
#include "frovedis/core/set_operations.hpp"
#include <cstdlib>

namespace cyclone::benchmarks {
  std::string random_string(const size_t len) {
    static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

    std::string tmp;
    tmp.reserve(len);

    for (auto i = 0; i < len; ++i) {
      tmp[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return tmp;
  }

  nullable_varchar_vector* create_string_input(int vector_size, int group_count) {
    // Generate the groups
    std::vector<std::string> groups(group_count);
    for (auto i = 0; i < groups.size(); i++) {
      groups[i] = random_string(rand() % 190 + 10);
    }

    // Generate the input
    std::vector<std::string> input(vector_size);
    for (auto i = 0; i < input.size(); i++) {
      input[i] = groups[i % group_count];
    }

    return new nullable_varchar_vector(input);
  }

  size_t vector_group1(nullable_varchar_vector *input) {
    auto groups = input->group_indexes();
    return groups.size();
  }

  size_t vector_group2(nullable_varchar_vector *input) {
    auto groups = input->group_indexes2();
    return groups.size();
  }

  size_t vector_group3(nullable_varchar_vector *input) {
    auto groups = input->group_indexes3();
    return groups.size();
  }

  TEST_CASE("String Group-by Implementation Benchmarks") {
    auto *input = create_string_input(3500000, 150);
    auto *input_with_invalids = create_string_input(3500000, 150);
    input_with_invalids->set_validity(1, 0);

    ankerl::nanobench::Bench().run("vector_group 1 (with validity, all valid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group1(input));
    });

    ankerl::nanobench::Bench().run("vector_group 1 (with validity, some invalid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group1(input_with_invalids));
    });

    ankerl::nanobench::Bench().run("vector_group 2 (with validity, all valid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group2(input));
    });

    ankerl::nanobench::Bench().run("vector_group 2 (with validity, some invalid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group2(input_with_invalids));
    });

    ankerl::nanobench::Bench().run("vector_group 3 (with validity, all valid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group3(input));
    });

    ankerl::nanobench::Bench().run("vector_group 3 (with validity, some invalid input)", [&]() {
      ankerl::nanobench::doNotOptimizeAway(vector_group3(input_with_invalids));
    });
  }
}
