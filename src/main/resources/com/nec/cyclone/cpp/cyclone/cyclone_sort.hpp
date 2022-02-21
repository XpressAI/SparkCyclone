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

#include "frovedis/core/radix_sort.hpp"
#include <array>
#include <tuple>
#include <vector>

namespace cyclone {
  namespace {
    template <std::size_t I, typename... Ts>
    void sort_by_ith_element(const std::vector<std::tuple<Ts...>> &elements,
                             const std::array<int, sizeof...(Ts)> &sort_order,
                             std::vector<size_t> &sorted_indices) {
      // Fetch the Ith type of the tuple
      using Type = std::tuple_element_t<I, std::tuple<Ts...>>;

      // Get all the Ith values from the vector of tuples
      std::vector<Type> temp(elements.size());
      for (auto i = 0; i < elements.size(); i++) {
        temp[i] = std::get<I>(elements[sorted_indices[i]]);
      }

      // Sort the Ith values of the tuples
      if (sort_order[I]) {
        frovedis::radix_sort(temp, sorted_indices);
      } else {
        frovedis::radix_sort_desc(temp, sorted_indices);
      }

      // Repeat the sort if needed
      if constexpr (I > 0) {
        sort_by_ith_element<I - 1, Ts...>(elements, sort_order, sorted_indices);
      }
    }
  }

  template <typename... Ts>
  std::vector<size_t> sort_tuples(const std::vector<std::tuple<Ts...>> &elements,
                                  const std::array<int, sizeof...(Ts)> &sort_order) {
    // Initialize the sorted indices
    std::vector<size_t> sorted_indices(elements.size());
    for (auto i = 0; i < elements.size(); i++) {
      sorted_indices[i] = i;
    }

    if constexpr (sizeof...(Ts) > 0) {
      // Apply sort from the N-1th to 0th element of the tuples,
      // which will update the sorted indices
      sort_by_ith_element<sizeof...(Ts) - 1, Ts...>(elements, sort_order, sorted_indices);
    }

    // Return the sorted indices
    return sorted_indices;
  }
}
