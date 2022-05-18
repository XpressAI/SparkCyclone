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
   * Append two bitsets using T as the holding type. T must be an unsigned
   * integer type.
   *
   * *first_tail* needs to be allocated such that T fits into it an integer
   * number of times.
   *
   * *second* does not need to be allocated to fit T an integer amount of times.
   * The last bytes will always be handled in a byte-wise fashion.
   *
   * Both *first_tail* and *second* must be aligned to sizeof(T), i.e. if
   * T=uint64_t then they **must** be aligned to 8 bytes, otherwise a
   * SIGBUS error will be thrown (signal 7).
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
    size_t bits_per_byte = CHAR_BIT;
    auto bits_per_T = sizeof(T) * bits_per_byte;
    //std::cout << "[append_bitsets] bits_per_T=" << bits_per_T << std::endl;

    // Simplest case: No dangling bits means we can straight copy the to be appended second bitset unto the tail of the
    // first bitset, as we have byte-granularity anyway
    if (dangling_bits == 0) {
      //std::cout << "[append_bitsets] byte-wise copy" << std::endl;

      auto bytes = frovedis::ceil_div(second_bit_count, bits_per_byte);
      //std::cout << "[append_bitsets] bytes=" << bytes << std::endl;
      //std::cout << "[append_bitsets] &first_tail=" << uintptr_t(first_tail) << std::endl;
      //std::cout << "[append_bitsets] &first_tail[bytes]=" << uintptr_t(first_tail) + bytes << std::endl;
      std::memcpy(first_tail, second, bytes);

      size_t out_dangling = second_bit_count % bits_per_T;
      //std::cout << "[append_bitsets] out_dangling=" << out_dangling << std::endl;
      //std::cout << "[append_bitsets] Done Copy " << std::endl;
      return out_dangling;
    }

    auto is_big_steps = sizeof(T) > 1;

    // How many elements *can* be fit into the tail
    auto space_in_tail = bits_per_T - dangling_bits;

    // How many elements *can not* fit into the tail
    auto additional_elements_to_fit = space_in_tail > second_bit_count ? 0 : second_bit_count - space_in_tail;

    // How many additional containers are necessary to fit the spill-over
    auto additional_container_count = frovedis::ceil_div(additional_elements_to_fit, bits_per_T);

    // How many steps *can* be done using the current T size?
    auto big_step_count = second_bit_count / bits_per_T;

    // How many elements need to be done with a byte step count?
    auto small_step_element_count = second_bit_count % bits_per_T;

    // Special case: We are already doing byte steps = small steps
    if(!is_big_steps){
      big_step_count += small_step_element_count > 0 ? 1 : 0;
      additional_container_count += additional_elements_to_fit > 0 ? 1 : 0;
      small_step_element_count = 0;
    }

    size_t out_dangling = (second_bit_count + dangling_bits) % bits_per_T;

    //std::cout << "[append_bitsets] second_bit_count=" << second_bit_count << std::endl;
    //std::cout << "[append_bitsets] dangling_bits=" << dangling_bits << std::endl;
    //std::cout << "[append_bitsets] is_big_steps=" << is_big_steps << std::endl;
    //std::cout << "[append_bitsets] space_in_tail=" << space_in_tail << std::endl;
    //std::cout << "[append_bitsets] additional_elements_to_fit=" << additional_elements_to_fit << std::endl;
    //std::cout << "[append_bitsets] additional_container_count=" << additional_container_count << std::endl;
    //std::cout << "[append_bitsets] big_step_count=" << big_step_count << std::endl;
    //std::cout << "[append_bitsets] small_step_element_count=" << small_step_element_count << std::endl;

    // Ensure space_in_tail is actually all empty and not some random crap
    // Building mask in a single step results in a signed shift, which we don't want
    T mask = ~0;
    mask = mask >> space_in_tail;

    //std::cout << "[append_bitsets] first_tail[0] = "<< std::bitset<sizeof(T)*CHAR_BIT>(first_tail[0]) << std::endl;
    //std::cout << "[append_bitsets] mask = "<< std::bitset<sizeof(T)*CHAR_BIT>(mask) << std::endl;
    first_tail[0] &= mask;
    //std::cout << "[append_bitsets] first_tail[0] & mask = "<< std::bitset<sizeof(T)*CHAR_BIT>(first_tail[0]) << std::endl;

    //std::cout << "[append_bitsets] setting up additional containers " << std::endl;
    // T steps
#pragma _NEC vector
#pragma _NEC ivdep
    for (size_t i = 0; i < additional_container_count; i++) {
      // Set the dangling bits of all follow up Ts
      first_tail[i + 1] = second[i] >> space_in_tail;
      //std::cout << "[append_bitsets] "<< std::bitset<sizeof(T)*CHAR_BIT>(first_tail[i + 1]) << " = " << std::bitset<sizeof(T)*CHAR_BIT>(second[i]) << " >> " << space_in_tail << std::endl;
    }

    //std::cout << "[append_bitsets] setting values " << std::endl;
#pragma _NEC vector
#pragma _NEC ivdep
    for (size_t i = 0; i < big_step_count; i++) {
      // Fill the free space in the tail of all Ts
      //std::cout << "[append_bitsets] "<< std::bitset<sizeof(T)*CHAR_BIT>(first_tail[i]) << " |= " << std::bitset<sizeof(T)*CHAR_BIT>(second[i]) << " << " << dangling_bits;
      first_tail[i] |= second[i] << dangling_bits;
      //std::cout << " = " << std::bitset<sizeof(T)*CHAR_BIT>(first_tail[i]) << std::endl;
    }

    // Byte-steps, if necessary
    if (small_step_element_count > 0) {
      //std::cout << "[append_bitsets] byte-wise recursion" << std::endl;

      //std::cout << "[append_bitsets] &first_tail=" << uintptr_t(first_tail) << std::endl;

      uint8_t *byte_second = reinterpret_cast<uint8_t *>(&second[big_step_count]);
      uint8_t *byte_tail_start = reinterpret_cast<uint8_t *>(&first_tail[big_step_count]);
      //std::cout << "[append_bitsets] &byte_second=" << uintptr_t(byte_second) << std::endl;
      //std::cout << "[append_bitsets] &byte_tail_start=" << uintptr_t(byte_tail_start) << std::endl;


      size_t first_non_full_byte = dangling_bits / bits_per_byte;
      //std::cout << "[append_bitsets] first_non_full_byte=" << first_non_full_byte << std::endl;
      uint8_t *byte_tail = &byte_tail_start[first_non_full_byte];
      //std::cout << "[append_bitsets] &byte_tail=" << uintptr_t(byte_tail) << std::endl;

      size_t dangling_byte_bits = dangling_bits % bits_per_byte;

      append_bitsets(byte_tail, dangling_byte_bits, byte_second, small_step_element_count);
    }

    //std::cout << "[append_bitsets] Done T = "<< sizeof(T) << std::endl;
    return out_dangling;
  }
}

template<typename T>
void fast_validity_merge(uint64_t *outbuf, T * const * const inputs, const size_t batches) {
  auto dangling_bits = 0;
  auto ox = 0;
  outbuf[0] = 0;

  for (auto b=0; b<batches; b++) {

    size_t wordcnt = inputs[b]->count / 64;
    size_t restcnt = inputs[b]->count  % 64;

    // if there is any rest bits to copy in wordcnt+1,
    // we copy the whole 64 bit as if they were fully used
    wordcnt += restcnt ? 1 : 0;

    uint64_t mask =  UINT64_MAX >> dangling_bits;
    uint64_t vmask = UINT64_MAX >> (64-restcnt);


    // copy whole words from source batch
    // since we might need to shift the bits by "dangling_bits" in the output
    // to not overwrite the odd bits at the end of the last merged input,
    // one source word might need to be split and written to 2 destination words
    for (auto i=0; i<wordcnt; i++) {

      uint64_t validity_bits = inputs[b]->validityBuffer[i];

      if (i == wordcnt - 1) validity_bits &= vmask;

      uint64_t lower_half = (validity_bits & mask) << dangling_bits;
      uint64_t upper_half = (validity_bits & ~mask) >> (64 - dangling_bits);

      outbuf[ox++] |= lower_half;
      outbuf[ox] = upper_half;

    }

    // now it might be, that the last word has not been used,
    // i.e. the upper_half above wasn't used at all and there are
    // a few bits left in the second to last word.
    // So, if there is at least one bit left, we need to
    // set back the output index ox to point to that location:
    ox = ox - 1 + (restcnt + dangling_bits) / 64;

    // update dangling_bits
    dangling_bits = (dangling_bits + restcnt) % 64;
   }
}