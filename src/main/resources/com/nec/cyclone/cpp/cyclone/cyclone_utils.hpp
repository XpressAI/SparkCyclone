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
#include "frovedis/dataframe/join.hpp"

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
   * Append two bitsets using T as the holding type.
   *
   * *first_tail* needs to be allocated such that T fits into it an integer
   * number of times.
   *
   * *second* does not need to be allocated to fit T an integer amount of times.
   * The last bytes will always be handled in a byte-wise fashion.
   *
   * @tparam T holding type
   * @param first_tail Pointer to last not yet full T of the first bitset
   * @param dangling_bits Count of bits already set in first_tail
   * @param second Pointer to the to-be-appended bitset
   * @param second_bit_count Count of bits in second
   * @return Count of bits set in the last T after appending
   */
  template<typename T>
  inline size_t append_bitsets(T* first_tail, size_t dangling_bits, T* second, size_t second_bit_count) {
    size_t bits_per_byte = 8;
    auto bits_per_T = sizeof(T) * bits_per_byte;
    std::cout << "[append_bitsets] bits_per_T=" << bits_per_T << std::endl;

    // Simplest case: No dangling bits means we can straight copy the to be appended second bitset unto the tail of the
    // first bitset, as we have byte-granularity anyway
    if (dangling_bits == 0) {
      std::cout << "[append_bitsets] byte-wise copy" << std::endl;

      auto bytes = frovedis::ceil_div(second_bit_count, bits_per_byte);
      std::cout << "[append_bitsets] bytes=" << bytes << std::endl;
      std::memcpy(first_tail, second, bytes);

      size_t out_dangling = second_bit_count % bits_per_T;
      std::cout << "[append_bitsets] out_dangling=" << out_dangling << std::endl;
      return out_dangling;
    }

    // Calculate how many full elements are necessary to fit the second bitset into the given type
    auto space_in_tail = bits_per_T - dangling_bits;
    auto elements_to_fit = second_bit_count - space_in_tail;
    auto full_elements = frovedis::ceil_div(elements_to_fit, bits_per_T);
    size_t out_dangling = elements_to_fit % bits_per_T;

    std::cout << "[append_bitsets] second_bit_count=" << second_bit_count << std::endl;
    std::cout << "[append_bitsets] dangling_bits=" << dangling_bits << std::endl;
    std::cout << "[append_bitsets] space_in_tail=" << space_in_tail << std::endl;
    std::cout << "[append_bitsets] elements_to_fit=" << elements_to_fit << std::endl;
    std::cout << "[append_bitsets] full_elements=" << full_elements << std::endl;
    std::cout << "[append_bitsets] out_dangling=" << out_dangling << std::endl;

    auto t_steps = full_elements;
    auto dangling_bytes = frovedis::ceil_div(out_dangling, bits_per_byte);
    if (dangling_bytes < sizeof(T)) {
      // Last step needs to be done byte-wise
      t_steps -= 1;
    }
    std::cout << "[append_bitsets] dangling_bytes=" << dangling_bytes << std::endl;
    std::cout << "[append_bitsets] t_steps=" << t_steps << std::endl;

    // T steps
#pragma _NEC vector
#pragma _NEC ivdep
    for (auto i = 0; i < t_steps; i++) {
      first_tail[i + 1] = second[i] >> space_in_tail;
      std::cout << "[append_bitsets] "<< first_tail[i + 1] << " = " << second[i] << " << " << space_in_tail << std::endl;
    }

#pragma _NEC vector
#pragma _NEC ivdep
    for (auto i = 0; i < t_steps; i++) {
      std::cout << "[append_bitsets] "<< first_tail[i] << " |= " << second[i] << " >> " << dangling_bits;
      first_tail[i] |= second[i] << dangling_bits;
      std::cout << " = " << first_tail[i] << std::endl;
    }

    // Byte-steps, if necessary
    if (t_steps != full_elements) {
      std::cout << "[append_bitsets] byte-wise recursion" << std::endl;

      char *byte_second = reinterpret_cast<char *>(&second[t_steps]);
      char *byte_tail_start = reinterpret_cast<char *>(&first_tail[t_steps]);

      size_t last_written_byte = frovedis::ceil_div(dangling_bits, bits_per_byte);
      size_t dangling_byte_bits = dangling_bits % bits_per_byte;
      char *byte_tail = &byte_tail_start[last_written_byte];

      append_bitsets(byte_tail, dangling_byte_bits, byte_second, out_dangling);
    }

    return out_dangling;
  }
}

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
