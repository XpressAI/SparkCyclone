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

namespace cyclone
{
  namespace
  {
    template <std::size_t I, typename... Ts>
    inline void sort_ith_element(const std::vector<std::tuple<Ts...>> &elements,
                                 const std::array<int, sizeof...(Ts)> &sort_order,
                                 std::vector<size_t> &sorted_indices)
    {
      // Fetch the Ith type of the tuple
      using Type = std::tuple_element_t<I, std::tuple<Ts...>>;

      // Get all the Ith values from the vector of tuples
      std::vector<Type> temp(elements.size());
#pragma _NEC ivdep
      for (auto i = 0; i < elements.size(); ++i)
      {
        temp[i] = std::get<I>(elements[sorted_indices[i]]);
      }

      // Sort the Ith values of the tuples
      if (sort_order[I])
      {
        frovedis::radix_sort(temp, sorted_indices);
      }
      else
      {
        frovedis::radix_sort_desc(temp, sorted_indices);
      }

      // Repeat the sort if needed
      if constexpr (I > 0)
      {
        sort_ith_element<I - 1, Ts...>(elements, sort_order, sorted_indices);
      }
    }
  }

  template <typename... Ts>
  inline std::vector<size_t> sort_tuples(const std::vector<std::tuple<Ts...>> &elements,
                                         const std::array<int, sizeof...(Ts)> &sort_order)
  {
    // Initialize the sorted indices
    std::vector<size_t> sorted_indices(elements.size());
#pragma _NEC vector
    for (auto i = 0; i < elements.size(); ++i)
    {
      sorted_indices[i] = i;
    }

    if constexpr (sizeof...(Ts) > 0)
    {
      // Apply sort from the N-1th to 0th element of the tuples,
      // which will update the sorted indices
      sort_ith_element<sizeof...(Ts) - 1, Ts...>(elements, sort_order, sorted_indices);
    }

    // Return the sorted indices
    return sorted_indices;
  }

  namespace
  {
    template <typename... Ts>
    inline std::vector<size_t> sort_columns_reversed_args(const size_t count,
                                                          const std::tuple<int, Ts> &...columns)
    {
      // Construct the sorted_indices
      std::vector<size_t> sorted_indices(count);

// Initialize by current order
#pragma _NEC vector
      for (auto i = 0; i < sorted_indices.size(); ++i)
      {
        sorted_indices[i] = i;
      }

      // Apply the fold expression to all the variadic args
      // This section will be expanded and inlined during compilation
      ([&](const auto &column)
       {
         // Extract the sort_order and the data pointer
         const auto &[sort_order, data] = column;

         // Construct the temp keys
         std::vector<std::remove_pointer_t<decltype(data)>> temp(sorted_indices.size());
#pragma _NEC ivdep
         for (auto i = 0; i < sorted_indices.size(); ++i)
         {
           temp[i] = data[sorted_indices[i]];
         }

         // Sort and apply ordering to the sorted_indices
         if (sort_order)
         {
           frovedis::radix_sort(temp, sorted_indices);
         }
         else
         {
           frovedis::radix_sort_desc(temp, sorted_indices);
         }
       }(columns),
       ...);

      return sorted_indices;
    }

    // Forward declaration of the struct for reversing the variadic args
    template <typename... Tn>
    struct reverse;

    // Base case - Apply sort_columns_reversed_args() to the (now-reversed) args
    template <>
    struct reverse<>
    {
      template <typename... Un>
      static inline std::vector<size_t> apply(const size_t size,
                                              const std::tuple<int, Un> &...un)
      {
        return sort_columns_reversed_args(size, un...);
      }
    };

    // Nth case - Swap the positions of the variadic args one by one
    template <typename T, typename... Tn>
    struct reverse<T, Tn...>
    {
      template <typename... Un>
      static inline std::vector<size_t> apply(const size_t size,
                                              const std::tuple<int, T> &t,
                                              const std::tuple<int, Tn> &...tn,
                                              const std::tuple<int, Un> &...un)
      {
        // Move the 1st parameter backwards
        return reverse<Tn...>::apply(size, tn..., t, un...);
      }
    };
  }

  template <typename... Ts>
  inline std::vector<size_t> sort_columns(const size_t count, const std::tuple<int, Ts> &...columns)
  {
    // The algorithm sorts starting from the N-1th column and moves down to the
    // 0th column.  A fold expressions is used to inline what would otherwise be
    // recursive function calls.  However, since fold expressions cannot be
    // applied in reverse order, we first reverse the order of the variadic args
    // before applying the sort function template.
    return reverse<Ts...>::apply(count, columns...);
  }
}
