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
#include <bitset>
#include <climits>
#include <cstring>
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include "frovedis/dataframe/join.hpp"

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

namespace cyclone {
  inline const std::vector<std::vector<size_t>> separate_to_groups(const std::vector<size_t> &ids, std::vector<size_t> &group_keys) {
    std::vector<size_t> groups = ids;
    frovedis::radix_sort(groups);
    std::vector<size_t> unique_groups = frovedis::set_unique(groups);
    std::vector<size_t> group_counts(unique_groups.size());

    auto ids_count = ids.size();
    auto ids_arr = ids.data();
    auto groups_arr = groups.data();
    auto groups_size = groups.size();
    auto groups_count_arr = group_counts.data();
    auto unique_groups_arr = unique_groups.data();
    auto unique_groups_size = unique_groups.size();

    // Count the number of groups
    for (size_t g = 0; g < unique_groups_size; g++) {
      size_t current_count = 0;
      size_t the_group = unique_groups_arr[g];

      #pragma _NEC vector
      for (size_t i = 0; i < groups_size; i++) {
        if (groups_arr[i] == the_group) {
          current_count++;
        }
      }
      groups_count_arr[g] = current_count;
    }

    std::vector<std::vector<size_t>> ret(unique_groups_size);
    for (size_t i = 0; i < unique_groups_size; i++) {
      ret[i].resize(groups_count_arr[i]);
    }

    auto ret_arr = ret.data();

    for (size_t g = 0; g < unique_groups_size; g++) {
      size_t group_pos = 0;
      size_t group = unique_groups_arr[g];
      auto ret_g_arr = ret_arr[g].data();
      #pragma _NEC vector
      for (size_t i = 0; i < ids_count; i++) {
        if (ids_arr[i] == group) {
          ret_g_arr[group_pos++] = i;
        }
      }
    }

    group_keys = unique_groups;
    return ret;
  }

  inline const std::vector<size_t> bitmask_to_matching_ids(const std::vector<size_t> &mask) {
    // Count the number of 1-bits
    size_t m_count = 0;
    #pragma _NEC vector
    for (int i = 0; i < mask.size(); i++) {
      m_count += mask[i];
    }

    // Allocate the output
    std::vector<size_t> output(m_count);

    // Append the positions for which the bit is 1
    // This loop will be vectorized on the VE as vector compress instruction (`vcp`)
    size_t pos = 0;
    #pragma _NEC vector
    for (int i = 0; i < mask.size(); i++) {
      if (mask[i]) {
        output[pos++] = i;
      }
    }

    return output;
  }

  inline void equi_join_indices(std::vector<size_t> &left, std::vector<size_t> &right, std::vector<size_t> &matchingLeft, std::vector<size_t> &matchingRight) {
      size_t left_len = left.size();
      size_t right_len = right.size();
      std::vector<size_t> left_idxs(left_len);
      std::vector<size_t> right_idxs(right_len);
      for(auto i = 0; i < left_len; i++){
          left_idxs[i] = i;
      }
      for(auto i = 0; i < right_len; i++){
          right_idxs[i] = i;
      }
      frovedis::equi_join(left, left_idxs, right, right_idxs, matchingLeft, matchingRight);
  }

  // Print out a std::tuple to ostream
  template<typename Ch, typename Tr, typename... Ts>
  auto& operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::tuple<Ts...> const &tup) {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "(";
    // Based on: https://stackoverflow.com/questions/6245735/pretty-print-stdtuple
    std::apply(
      [&tmp] (auto&&... args) { ((tmp << args << ", "), ...); },
      tup
    );
    tmp.seekp(-2, tmp.cur);
    tmp << ")";
    return stream << tmp.str();
  }

  // Print out a std::vector to ostream
  // Define this AFTER defining operator<< for std::tuple
  // so that we can print std::vector<std::tuple<Ts...>>
  template<typename Ch, typename Tr, typename T>
  auto& operator<<(std::basic_ostream<Ch, Tr> &stream,
                   std::vector<T> const &vec) {
    std::basic_stringstream<Ch, Tr> tmp;
    tmp << "[ ";
    for (const auto &elem : vec) {
      tmp << elem << ", ";
    }
    tmp.seekp(-2, tmp.cur);
    tmp << " ]";
    return stream << tmp.str();
  }

  template<typename T>
  void print_vec(const std::string &name, const std::vector<T> &vec) {
    std::cout << name <<  " = " << vec << std::endl;
  }


  /**
   * Append two bitsets using uint64_t as the container type.
   *
   * Both first and second need to be vector aligned and fit the container type an integer number of times.
   * If they are not aligned, a SIGBUS (signal 7) error will be thrown.
   * If they don't fit the container type fully, memory corruption will occur.
   *
   * Example based on 8-bit container (note: little endianness!):
   *
   *     7......0 container size: 8 bits
   *          111
   *          ^^^ spilled: 3 bits
   * 111 11111
   *     ^^^^^ fits into previous container
   * ^^^ needs to spill over into next container
   *
   * @param first pointer to first bitset. Must have enough space to contain second_bit_count additional elements
   * @param first_bit_count count of bits in first bitset
   * @param second Pointer to the to-be-appended bitset
   * @param second_bit_count Count of bits in second
   */
  inline void append_bitsets(uint64_t* first, size_t first_bit_count, uint64_t* second, size_t second_bit_count){
    size_t bits_per_container = sizeof(uint64_t) * CHAR_BIT;

    size_t tail_pos = first_bit_count / bits_per_container;
    uint64_t* tail = &first[tail_pos];

    size_t spilled_bits = first_bit_count % bits_per_container;
    // when spilled_bits = 0, we can directly copy the data without any shifting
    // (also shifting by > 63 is undefined, so we should take a defined route anyway)
    if(spilled_bits == 0){
      size_t bytes = frovedis::ceil_div(second_bit_count, bits_per_container) * sizeof(uint64_t);
      std::memcpy(tail, second, bytes);
    }else{
      size_t prev_space = bits_per_container - spilled_bits;

      size_t step_count = frovedis::ceil_div(second_bit_count, bits_per_container);
      size_t additional_container_count = second_bit_count < prev_space ? 0 : frovedis::ceil_div(second_bit_count - prev_space, bits_per_container);

      // Apply mask to ensure empty space is zero initialized
      uint64_t mask = ~0; // Initialize to all 1
      mask = mask >> prev_space; // Move by empty space to zero out empty spaces
      tail[0] &= mask;

      // Prepare additional containers, by setting their initial value to the spill-over from the previous container
#pragma _NEC vector
#pragma _NEC ivdep
      for(size_t i = 0; i < additional_container_count; i++){
        tail[i + 1] = second[i] >> prev_space;
      }

      // Set values into empty space
#pragma _NEC vector
#pragma _NEC ivdep
      for(size_t i = 0; i < step_count; i++){
        tail[i] |= second[i] << spilled_bits;
      }
    }
  }
}
