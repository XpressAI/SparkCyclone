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

#include "cyclone/transfer-definitions.hpp"
#include "cyclone/cyclone.hpp"
#include "frovedis/core/utility.hpp"
#include <stdlib.h>

template <typename T>
bool NullableScalarVec<T>::equals(const NullableScalarVec<T> * const other) {
  // Compare count
  auto output = (count == other->count);

  // Compare data
  for (auto i = 0; i < count; i++) {
    output = output && (data[i] == other->data[i]);
  }

  // Compare validityBuffer
  for (auto i = 0; i < count; i++) {
    output = output && (check_valid(validityBuffer, i) == check_valid(other->validityBuffer, i));
  }

  return output;
}

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::clone() {
  // Allocate
  auto * output = static_cast<NullableScalarVec<T> *>(malloc(sizeof(NullableScalarVec<T>)));

  // Copy the count
  output->count = count;

  // Copy the data
  auto dbytes = output->count * sizeof(T);
  output->data = static_cast<T *>(malloc(dbytes));
  memcpy(output->data, data, dbytes);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

bool nullable_varchar_vector::equals(const nullable_varchar_vector * const other) {
  // Compare count
  auto output = (count == other->count);

  // Compare dataSize
  output = output && (dataSize == other->dataSize);

  // Compare data
  for (auto i = 0; i < count; i++) {
    output = output && (data[i] == other->data[i]);
  }

  // Compare offsets
  for (auto i = 0; i < count + 1; i++) {
    output = output && (offsets[i] == other->offsets[i]);
  }

  // Compare validityBuffer
  for (auto i = 0; i < count; i++) {
    output = output && (check_valid(validityBuffer, i) == check_valid(other->validityBuffer, i));
  }

  return output;
}

nullable_varchar_vector * nullable_varchar_vector::clone() {
  // Allocate
  auto * output = static_cast<nullable_varchar_vector *>(malloc(sizeof(nullable_varchar_vector)));

  // Copy the count and dataSizes
  output->count = count;
  output->dataSize = dataSize;

  // Copy the data
  output->data = static_cast<char *>(malloc(output->dataSize));
  memcpy(output->data, data, output->dataSize);

  // Copy the offsets
  auto obytes = (output->count + 1) * sizeof(int32_t);
  output->offsets = static_cast<int32_t *>(malloc(obytes));
  memcpy(output->offsets, offsets, obytes);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

// Forward declare the class template instantiations to prevent linker errors:
// https://stackoverflow.com/questions/3008541/template-class-symbols-not-found
template class NullableScalarVec<int32_t>;
template class NullableScalarVec<int64_t>;
template class NullableScalarVec<float>;
template class NullableScalarVec<double>;
