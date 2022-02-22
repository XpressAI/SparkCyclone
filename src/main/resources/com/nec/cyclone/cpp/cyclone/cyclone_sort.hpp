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
#include <type_traits>
#include <vector>

namespace cyclone {
  namespace {
    template <std::size_t I, typename... Ts>
    inline void sort_ith_element(const std::vector<std::tuple<Ts...>> &elements,
                                    const std::array<int, sizeof...(Ts)> &sort_order,
                                    std::vector<size_t> &sorted_indices) {
      // Fetch the Ith type of the tuple
      using Type = std::tuple_element_t<I, std::tuple<Ts...>>;

      // Get all the Ith values from the vector of tuples
      std::vector<Type> temp(elements.size());
      #pragma _NEC ivdep
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
        sort_ith_element<I - 1, Ts...>(elements, sort_order, sorted_indices);
      }
    }
  }

  template <typename... Ts>
  inline std::vector<size_t> sort_tuples(const std::vector<std::tuple<Ts...>> &elements,
                                         const std::array<int, sizeof...(Ts)> &sort_order) {
    // Initialize the sorted indices
    std::vector<size_t> sorted_indices(elements.size());
    #pragma _NEC vector
    for (auto i = 0; i < elements.size(); i++) {
      sorted_indices[i] = i;
    }

    if constexpr (sizeof...(Ts) > 0) {
      // Apply sort from the N-1th to 0th element of the tuples,
      // which will update the sorted indices
      sort_ith_element<sizeof...(Ts) - 1, Ts...>(elements, sort_order, sorted_indices);
    }

    // Return the sorted indices
    return sorted_indices;
  }

  namespace {
    template<typename T>
    inline void sort_ith_column(std::vector<size_t> &sorted_indices,
                                const T * const data) {
      std::vector<T> temp(sorted_indices.size());
      #pragma _NEC vector
      for (auto i = 0; i < sorted_indices.size(); i++) {
        temp[i] = data[sorted_indices[i]];
      }

      frovedis::radix_sort(temp, sorted_indices);
    }

    template<typename T, typename ...Ts>
    inline void sort_ith_column(std::vector<size_t> &sorted_indices,
                                const T * const data,
                                const Ts * const ...tail) {
      sort_ith_column(sorted_indices, tail...);

      std::vector<T> temp(sorted_indices.size());
      #pragma _NEC vector
      for (auto i = 0; i < sorted_indices.size(); i++) {
        temp[i] = data[sorted_indices[i]];
      }

      frovedis::radix_sort(temp, sorted_indices);
    }
  }

  template <typename... Ts>
  inline std::vector<size_t> sort_tuples(const size_t count,
                                         const Ts * const ...datum) {
    // Initialize the sorted indices
    std::vector<size_t> sorted_indices(count);
    #pragma _NEC vector
    for (auto i = 0; i < sorted_indices.size(); i++) {
      sorted_indices[i] = i;
    }

    if constexpr (sizeof...(Ts) > 0) {
      // Apply sort from the N-1th to 0th element of the tuples,
      // which will update the sorted indices
      sort_ith_column(sorted_indices, datum...);
    }

    // Return the sorted indices
    return sorted_indices;
  }




  namespace {
    template <typename ...Ts>
    inline std::vector<size_t> sort_tuples_reverse(const size_t count, const std::tuple<int, Ts> &...columns) {
      std::vector<size_t> sorted_indices(count);

      #pragma _NEC vector
      for (auto i = 0; i < sorted_indices.size(); i++) {
        sorted_indices[i] = i;
      }

      // Run fold expression - this will be expanded and inlined during compilation
      ([&] (const auto & column) {
        const auto & [ sort_order, data ] = column;

        std::vector<std::remove_pointer_t<decltype(data)>> temp(sorted_indices.size());
        #pragma _NEC vector
        for (auto i = 0; i < sorted_indices.size(); i++) {
          temp[i] = data[sorted_indices[i]];
        }

        if (sort_order) {
          frovedis::radix_sort(temp, sorted_indices);
        } else {
          frovedis::radix_sort_desc(temp, sorted_indices);
        }

      } (columns), ...);

      return sorted_indices;
    }

    // Forward declaration
    template<typename ...Tn>
    struct reverse;

    // Base case - apply sort_tuples_reverse
    template<>
    struct reverse<> {
      template<typename ...Un>
      static inline std::vector<size_t> apply(const size_t size,
                                              const std::tuple<int, Un> &...un) {
        return sort_tuples_reverse(size, un...);
      }
    };

    // Nth case - swap the variadic args' positions
    template<typename T, typename ...Tn>
    struct reverse<T, Tn...> {
      template<typename ...Un>
      static inline std::vector<size_t> apply(const size_t size,
                                              const std::tuple<int, T> &t,
                                              const std::tuple<int, Tn> &...tn,
                                              const std::tuple<int, Un> &...un) {
        // Bubble the 1st parameter backwards
        return reverse<Tn...>::apply(size, tn..., t, un...);
      }
    };
  }

  template <typename ...Ts>
  inline std::vector<size_t> sort_tuples(const size_t count, const std::tuple<int, Ts> &...columns) {
    return reverse<Ts...>::apply(count, columns...);


    // std::vector<size_t> sorted_indices(count);

    // #pragma _NEC vector
    // for (auto i = 0; i < sorted_indices.size(); i++) {
    //   sorted_indices[i] = i;
    // }

    // ([&] (const auto & column) {
    //     const auto & [ sort_order, data ] = column;

    //     std::vector<std::remove_pointer_t<Ts>> temp(sorted_indices.size());
    //     #pragma _NEC vector
    //     for (auto i = 0; i < sorted_indices.size(); i++) {
    //       temp[i] = data[sorted_indices[i]];
    //     }

    //     if (sort_order) {
    //       frovedis::radix_sort(temp, sorted_indices);
    //     } else {
    //       frovedis::radix_sort_desc(temp, sorted_indices);
    //     }
    // } (columns), ...);

    // return sorted_indices;
  }
}
