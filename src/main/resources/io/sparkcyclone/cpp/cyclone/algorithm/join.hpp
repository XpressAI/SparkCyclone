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

#include <vector>
#include "frovedis/dataframe/join.hpp"

namespace cyclone::join {
  inline void equi_join_indices(std::vector<size_t> &left,
                                std::vector<size_t> &right,
                                std::vector<size_t> &matchingLeft,
                                std::vector<size_t> &matchingRight) {
    size_t left_len = left.size();
    size_t right_len = right.size();

    std::vector<size_t> left_idxs(left_len);
    std::vector<size_t> right_idxs(right_len);

    for (auto i = 0; i < left_len; i++) {
      left_idxs[i] = i;
    }

    for (auto i = 0; i < right_len; i++) {
      right_idxs[i] = i;
    }

    frovedis::equi_join(left, left_idxs, right, right_idxs, matchingLeft, matchingRight);
  }
}
