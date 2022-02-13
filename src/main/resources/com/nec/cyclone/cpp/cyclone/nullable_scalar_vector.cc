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
#include "cyclone/transfer-definitions.hpp"
#include "cyclone/cyclone.hpp"
#include "frovedis/core/utility.hpp"
#include <stdlib.h>
#include <iostream>

template <typename T>
NullableScalarVec<T>::NullableScalarVec(const std::vector<T> &src) {
  // Initialize count
  count = src.size();

  // Copy the data
  data = static_cast<T *>(malloc(sizeof(T) * src.size()));
  for (auto i = 0; i < src.size(); i++) {
    data[i] = src[i];
  }

  // Set the validityBuffer
  size_t vcount = ceil(src.size() / 64.0);
  validityBuffer = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * vcount));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

template <typename T>
void NullableScalarVec<T>::reset() {
  // Free the owned memory
  free(data);
  free(validityBuffer);

  // Reset the pointers and values
  data            = nullptr;
  validityBuffer  = nullptr;
  count           = 0;
}

template <typename T>
bool NullableScalarVec<T>::is_default() const {
  return data == nullptr &&
    validityBuffer  == nullptr &&
    count == 0;
}


template <typename T>
void NullableScalarVec<T>::print() const {
  std::stringstream stream;

  stream << "NullableScalarVec<T> @ " << this << " {\n";
  // Print count
  stream << "  COUNT: " << count << "\n";

  if (count <= 0) {
    stream << "  DATA: [ ]\n"
           << "  VALIDITY: [ ]\n";
  } else {
    // Print data
    stream << "  DATA: [ ";
    for (auto i = 0; i < count; i++) {
      if (check_valid(validityBuffer, i)) {
        stream << data[i] << ", ";
      } else {
        stream << "#, ";
      }
    }

    // Print validityBuffer
    stream << "]\n  VALIDITY: [";
    for (auto i = 0; i < count; i++) {
        stream << check_valid(validityBuffer, i) << ", ";
    }
    stream << "]\n";
  }

  stream << "}\n";
  std::cout << stream.str() << std::endl;
}

template <typename T>
bool NullableScalarVec<T>::equals(const NullableScalarVec<T> * const other) const {
  if (is_default() && other->is_default()) {
    return true;
  }

  // Compare count
  auto output = (count == other->count);

  // Compare data
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (data[i] == other->data[i]);
  }

  // Compare validityBuffer
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (check_valid(validityBuffer, i) == check_valid(other->validityBuffer, i));
  }

  return output;
}

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::clone() const {
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

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::filter(const std::vector<size_t> &matching_ids) const {
  // Allocate
  auto * output = static_cast<NullableScalarVec<T> *>(malloc(sizeof(NullableScalarVec<T>)));

  // Set the count
  output->count = matching_ids.size();

  // Allocate data
  auto dbytes = output->count * sizeof(T);
  output->data = static_cast<T *>(malloc(dbytes));

  // Allocate validityBuffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));

  // Preserve the validityBuffer across the filter
  #pragma _NEC vector
  for (int o = 0; o < matching_ids.size(); o++) {
    // Fetch the original index
    int i = matching_ids[o];

    // Copy the data unconditionally (allows for loop vectorization)
    output->data[o] = data[i];

    // Copy the validity buffer
    output->set_validity(o, get_validity(i));
  }

  return output;
}

template <typename T>
NullableScalarVec<T> ** NullableScalarVec<T>::bucket(const std::vector<size_t> &bucket_counts,
                                                     const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of NullableScalarVec<T> pointers
  auto ** output = static_cast<NullableScalarVec<T> **>(malloc(sizeof(T *) * bucket_counts.size()));

  // Loop over each bucket
  for (int b = 0; b < bucket_counts.size(); b++) {
    // Generate the list of indexes where the bucket assignment is b
    std::vector<size_t> matching_ids(bucket_counts[b]);
    {
      // This loop will be vectorized on the VE as vector compress instruction (`vcp`)
      size_t pos = 0;
      #pragma _NEC vector
      for (int i = 0; i < bucket_assignments.size(); i++) {
        if (bucket_assignments[i] == b) {
          matching_ids[pos++] = i;
        }
      }
    }

    // Create a filtered copy based on the list of indexes
    output[b] = this->filter(matching_ids);
  }

  return output;
}

// Forward declare the class template instantiations to prevent linker errors:
// https://stackoverflow.com/questions/3008541/template-class-symbols-not-found
template class NullableScalarVec<int32_t>;
template class NullableScalarVec<int64_t>;
template class NullableScalarVec<float>;
template class NullableScalarVec<double>;