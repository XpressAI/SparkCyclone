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

namespace cyclone::bitset {
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
  inline void append_bitsets(uint64_t * first,
                             const size_t first_bit_count,
                             const uint64_t * second,
                             const size_t second_bit_count) {
    size_t bits_per_container = sizeof(uint64_t) * CHAR_BIT;

    size_t tail_pos = first_bit_count / bits_per_container;
    uint64_t * tail = & first[tail_pos];

    size_t spilled_bits = first_bit_count % bits_per_container;
    // when spilled_bits = 0, we can directly copy the data without any shifting
    // (also shifting by > 63 is undefined, so we should take a defined route anyway)
    if (spilled_bits == 0) {
      size_t bytes = frovedis::ceil_div(second_bit_count, bits_per_container) * sizeof(uint64_t);
      std::memcpy(tail, second, bytes);

    } else {
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
      for (size_t i = 0; i < additional_container_count; i++) {
        tail[i + 1] = second[i] >> prev_space;
      }

      // Set values into empty space
      #pragma _NEC vector
      #pragma _NEC ivdep
      for (size_t i = 0; i < step_count; i++) {
        tail[i] |= second[i] << spilled_bits;
      }
    }
  }

  template<typename T>
  void fast_validity_merge(uint64_t *outbuf, T * const * const inputs, const size_t batches) {
    size_t bit_output_cnt = 0;

    for (auto b = 0; b < batches; b++) {
      size_t inputs_cnt = inputs[b] -> count;

      append_bitsets(
        outbuf,
        bit_output_cnt,
        inputs[b] -> validityBuffer,
        inputs_cnt
      );

      bit_output_cnt += inputs_cnt;
    }
  }
}
