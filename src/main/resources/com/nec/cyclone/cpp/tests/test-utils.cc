/*
 * Copyright (c) 2021 Xpress AI.
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

#include "tests/test-utils.hpp"
#include <math.h>
#include <iostream>

namespace cyclone::tests {
  nullable_varchar_vector * to_nullable_varchar_vector(const std::vector<std::string> &data) {
    auto *vec = new nullable_varchar_vector;

    // Initialize count
    vec->count = data.size();

    // Initialize dataSize
    vec->dataSize = 0;
    for (auto i = 0; i < data.size(); i++) {
      vec->dataSize += data[i].size();
    }

    // Copy strings to data
    vec->data = new char[vec->dataSize];
    auto p = 0;
    for (auto i = 0; i < data.size(); i++) {
      for (auto j = 0; j < data[i].size(); j++) {
        vec->data[p++] = data[i][j];
      }
    }

    // Set the offsets
    vec->offsets = new int32_t[data.size() + 1];
    vec->offsets[0] = 0;
    for (auto i = 0; i < data.size(); i++) {
      vec->offsets[i+1] = vec->offsets[i] + data[i].size();
    }

    // Set the validityBuffer
    size_t vcount = ceil(data.size() / 64.0);
    vec->validityBuffer = new uint64_t[vcount];
    for (auto i = 0; i < vcount; i++) {
      vec->validityBuffer[i] = 0xffffffffffffffff;
    }

    return vec;
  }

  template <typename T>
  NullableScalarVec<T> * to_nullable_scalar_vector(const std::vector<T> &data) {
    auto *vec = new NullableScalarVec<T>;

    // Initialize count
    vec->count = data.size();

    // Copy the data
    vec->data = new T[data.size()];
    for (auto i = 0; i < data.size(); i++) {
      vec->data[i] = data[i];
    }

    // Set the validityBuffer
    size_t vcount = ceil(data.size() / 64.0);
    vec->validityBuffer = new uint64_t[vcount];
    for (auto i = 0; i < vcount; i++) {
      vec->validityBuffer[i] = 0xffffffffffffffff;
    }

    return vec;
  }

  // Forward declare the function template instantiations to prevent linker errors
  template NullableScalarVec<int32_t> * to_nullable_scalar_vector(const std::vector<int32_t> &data);
  template NullableScalarVec<int64_t> * to_nullable_scalar_vector(const std::vector<int64_t> &data);
  template NullableScalarVec<float> * to_nullable_scalar_vector(const std::vector<float> &data);
  template NullableScalarVec<double> * to_nullable_scalar_vector(const std::vector<double> &data);
}
