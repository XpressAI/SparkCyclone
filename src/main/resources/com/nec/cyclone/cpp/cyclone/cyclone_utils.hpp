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

#include <stddef.h>
#include <iostream>
#include <tuple>
#include <vector>
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"

namespace cyclone
{
  inline const std::vector<std::vector<size_t>> separate_to_groups(const std::vector<size_t> &ids, std::vector<size_t> &group_keys)
  {
    std::vector<size_t> groups = ids;
    frovedis::radix_sort(groups);
    std::vector<size_t> unique_groups = frovedis::set_unique(groups);
    std::vector<size_t> group_counts(unique_groups.size());

    // Count the number of groups
    for (size_t g = 0; g < unique_groups.size(); ++g)
    {
      size_t current_count = -1;
#pragma _NEC vector
      for (size_t i = 0; i < groups.size(); ++i)
      {
        if (groups[i] == g)
        {
          ++current_count;
        }
      }
      group_counts[g] = current_count;
    }

    std::vector<std::vector<size_t>> ret(unique_groups.size());
    for (size_t i = 0; i < ret.size(); ++i)
    {
      ret[i].resize(group_counts[i]);
    }

    for (size_t g = 0; g < unique_groups.size(); ++g)
    {
      size_t group_pos = -1;
      size_t group = unique_groups[g];
#pragma _NEC vector
      for (size_t i = 0; i < ids.size(); ++i)
      {
        if (ids[i] == group)
        {
          ret[g][++group_pos] = i;
        }
      }
    }

    group_keys = unique_groups;
    return ret;
  }

  inline const std::vector<size_t> bitmask_to_matching_ids(const std::vector<size_t> &mask)
  {
    // Count the number of 1-bits
    size_t m_count = 0;
#pragma _NEC vector
    for (int i = 0; i < mask.size(); ++i)
    {
      m_count += mask[i];
    }

    // Allocate the output
    std::vector<size_t> output(m_count);

    // Append the positions for which the bit is 1
    // This loop will be vectorized on the VE as vector compress instruction (`vcp`)
    size_t pos = -1;
#pragma _NEC vector
    for (int i = 0; i < mask.size(); ++i)
    {
      if (mask[i])
      {
        output[++pos] = i;
      }
    }

    return output;
  }

  // Print out a std::tuple to ostream
  template <typename Ch, typename Tr, typename... Ts>
  auto &operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::tuple<Ts...> const &tup)
  {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "(";
    // Based on: https://stackoverflow.com/questions/6245735/pretty-print-stdtuple
    std::apply(
        [&tmp](auto &&...args)
        { ((tmp << args << ", "), ...); },
        tup);
    tmp.seekp(-2, tmp.cur);
    tmp << ")";
    return stream << tmp.str();
  }

  // Print out a std::vector to ostream
  // Define this AFTER defining operator<< for std::tuple
  // so that we can print std::vector<std::tuple<Ts...>>
  template <typename Ch, typename Tr, typename T>
  auto &operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::vector<T> const &vec)
  {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "[ ";
    for (const auto &elem : vec)
    {
      tmp << elem << ", ";
    }
    tmp.seekp(-2, tmp.cur);
    tmp << " ]";
    return stream << tmp.str();
  }
}

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
