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
#pragma once

#include "cyclone/cyclone.hpp"
#include "tests/doctest.h"
#include <tuple>
#include <vector>

namespace cyclone::tests {
  const std::vector<std::tuple<int32_t, float, int64_t, double>> elements {
    std::make_tuple(0, 7.29214f,    3ll, 9.850428l),
    std::make_tuple(0, 5.1007037f,  2ll, 2.1967127l),
    std::make_tuple(1, 2.106764f,   2ll, 2.029292l),
    std::make_tuple(0, 7.29214f,    3ll, 1.6248848l),
    std::make_tuple(2, 4.0789514f,  3ll, 5.4606824l),
    std::make_tuple(2, 2.1760006f,  6ll, 7.483787l),
    std::make_tuple(1, 4.6863422f,  3ll, 1.2526741l),
    std::make_tuple(1, 7.768214f,   6ll, 5.3003526l),
    std::make_tuple(2, 7.5028214f,  2ll, 9.859546l),
    std::make_tuple(2, 3.648821f,   1ll, 3.5679564l),
    std::make_tuple(1, 3.7641335f,  5ll, 0.024474824l),
    std::make_tuple(2, 8.525803f,   7ll, 3.0891323l),
    std::make_tuple(1, 2.5747693f,  4ll, 7.011116l),
    std::make_tuple(2, 0.57439226f, 5ll, 7.136852l),
    std::make_tuple(0, 2.5966463f,  2ll, 9.806966l),
    std::make_tuple(0, 6.291629f,   0ll, 5.715928l),
    std::make_tuple(1, 0.5641213f,  4ll, 0.5068447l),
    std::make_tuple(0, 9.341458f,   4ll, 4.637736l),
    std::make_tuple(0, 7.292144f,   3ll, 2.9226866l),
    std::make_tuple(0, 7.5230184f,  6ll, 2.865592l)
  };

  TEST_SUITE("std::tuple sort") {
    TEST_CASE("Tuple sort works") {
      // Manually sort the tuples by the N-1th, N-2th, ... 0th elements
      std::vector<size_t> expected(elements.size());
      {
        for (int i = 0; i < elements.size(); i++) {
          expected[i] = i;
        }

        {
          std::vector<double> temp(elements.size());
          for (auto i = 0; i < elements.size(); i++) {
            temp[i] = std::get<3>(elements[expected[i]]);
          }
          frovedis::radix_sort(temp, expected);
        }
        {
          std::vector<int64_t> temp(elements.size());
          for (auto i = 0; i < elements.size(); i++) {
            temp[i] = std::get<2>(elements[expected[i]]);
          }
          frovedis::radix_sort(temp, expected);
        }
        {
          std::vector<float> temp(elements.size());
          for (auto i = 0; i < elements.size(); i++) {
            temp[i] = std::get<1>(elements[expected[i]]);
          }
          frovedis::radix_sort(temp, expected);
        }
        {
          std::vector<int32_t> temp(elements.size());
          for (auto i = 0; i < elements.size(); i++) {
            temp[i] = std::get<0>(elements[expected[i]]);
          }
          frovedis::radix_sort(temp, expected);
        }
      }

      // Perform the same sorting using template function generation
      const auto sorted_indices1 = cyclone::sort_tuples(elements, std::array<int, 4> {{ 1, 1, 1, 1 }});
      // Perform a different sorting, where the 0th elementh is sorted in DESC order
      const auto sorted_indices2 = cyclone::sort_tuples(elements, std::array<int, 4> {{ 0, 1, 1, 1 }});

      CHECK(sorted_indices1 == expected);
      CHECK(sorted_indices2 != expected);
    }

    TEST_CASE("Tuple sort works for empty tuples") {
      const std::vector<std::tuple<>> elements {
        std::tuple<>(),
        std::tuple<>(),
        std::tuple<>()
      };

      std::vector<size_t> expected(elements.size());
      {
        for (int i = 0; i < elements.size(); i++) {
          expected[i] = i;
        }
      }

      const auto sorted_indices = cyclone::sort_tuples(elements, std::array<int, 0> {{ }});
      CHECK(sorted_indices == expected);
    }
  }
}
