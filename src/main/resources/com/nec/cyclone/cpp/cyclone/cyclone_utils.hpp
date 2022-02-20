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
#include <vector>

namespace cyclone {
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
}
