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
#include "cyclone/transfer-definitions.hpp"
#include "frovedis/core/utility.hpp"
#include <stdlib.h>
#include <iostream>

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::allocate() {
  // Allocate
  auto *output = static_cast<NullableScalarVec<T> *>(malloc(sizeof(NullableScalarVec<T>)));
  if (output == nullptr) {
    std::cout << "NullableScalarVec<T>::allocate() failed." << std::endl;
  }

  // Initialize
  return new (output) NullableScalarVec<T>;
}

template <typename T>
NullableScalarVec<T>::NullableScalarVec(const std::vector<T> &src) {
  // Initialize count
  count = src.size();

  // Copy the data
  data = static_cast<T *>(malloc(sizeof(T) * src.size()));
  if (data == nullptr) {
    std::cout << "NullableScalarVec<T>::__ctor() (data) failed." << std::endl;
  }
  for (auto i = 0; i < src.size(); i++) {
    data[i] = src[i];
  }

  // Set the validityBuffer
  size_t vcount = ceil(src.size() / 64.0);
  validityBuffer = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * vcount));
  if (validityBuffer == nullptr) {
    std::cout << "NullableScalarVec<T>::__ctor() (validity) failed." << std::endl;
  }
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
void NullableScalarVec<T>::resize(const size_t size) {
  // Reset the pointers and values
  reset();

  // Set count
  count = size;

  // Set data to new buffer
  data = static_cast<T *>(malloc(sizeof(T) * count));
  if (data == nullptr) {
    std::cout << "NullableScalarVec<T>::resize() (data) failed." << std::endl;
  }

  // Set validityBuffer to new buffer
  auto vbytes = frovedis::ceil_div(count, int32_t(64)) * sizeof(uint64_t);
  validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  if (validityBuffer == nullptr) {
    std::cout << "NullableScalarVec<T>::__ctor() (validity) failed." << std::endl;
  }
}

template <typename T>
void NullableScalarVec<T>::move_assign_from(NullableScalarVec<T> * other) {
  // Reset the pointers and values
  reset();

  // Assign the pointers and values from other
  data            = other->data;
  validityBuffer  = other->validityBuffer;
  count           = other->count;

  // Free the other (struct only)
  free(other);
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
      if (get_validity(i)) {
        stream << data[i] << ", ";
      } else {
        stream << "#, ";
      }
    }

    // Print validityBuffer
    stream << "]\n  VALIDITY: [";
    for (auto i = 0; i < count; i++) {
        stream << get_validity(i) << ", ";
    }
    stream << "]\n";
  }

  stream << "}\n";
  std::cout << stream.str() << std::endl;
}

template <typename T>
const std::vector<int32_t> NullableScalarVec<T>::validity_vec() const {
  std::vector<int32_t> bitmask(count);

  #pragma _NEC vector
  for (auto i = 0; i < count; i++) {
    bitmask[i] = get_validity(i);
  }

  return bitmask;
}

template <typename T>
const std::vector<size_t> NullableScalarVec<T>::size_t_data_vec() const {
  std::vector<size_t> output(count);

  #pragma _NEC vector
  for (auto i = 0; i < count; i++) {
    // Note: No explicit sign checking is performed here in order to keep the loop fast.
    output[i] = data[i];
  }

  return output;
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
    output = output && (get_validity(i) == other->get_validity(i));
  }

  return output;
}

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::clone() const {
  // Allocate
  auto *output = allocate();

  // Copy the count
  output->count = count;

  // Copy the data
  auto dbytes = output->count * sizeof(T);
  output->data = static_cast<T *>(malloc(dbytes));
  if (output->data == nullptr) {
    std::cout << "NullableScalarVec<T>::clone() (data) failed." << std::endl;
  }
  memcpy(output->data, data, dbytes);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  if (output->data == nullptr) {
    std::cout << "NullableScalarVec<T>::clone() (validity) failed." << std::endl;
  }
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::select(const std::vector<size_t> &selected_ids) const {
  // Allocate
  auto *output = allocate();

  // Set the count
  output->count = selected_ids.size();

  // Allocate data
  auto dbytes = output->count * sizeof(T);
  output->data = static_cast<T *>(malloc(dbytes));
  if (output->data == nullptr) {
    std::cout << "NullableScalarVec<T>::select() (data) failed." << std::endl;
  }

  // Allocate validityBuffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  if (output->validityBuffer == nullptr) {
    std::cout << "NullableScalarVec<T>::select() (validity) failed." << std::endl;
  }

  // Preserve the validityBuffer across the select
  #pragma _NEC vector
  for (auto o = 0; o < selected_ids.size(); o++) {
    // Fetch the original index
    int i = selected_ids[o];

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
  auto **output = static_cast<NullableScalarVec<T> **>(malloc(sizeof(T *) * bucket_counts.size()));
  if (output == nullptr) {
    std::cout << "NullableScalarVec<T>::bucket() (output) failed." << std::endl;
  }

  // Loop over each bucket
  for (auto b = 0; b < bucket_counts.size(); b++) {
    // Generate the list of indexes where the bucket assignment is b
    std::vector<size_t> selected_ids(bucket_counts[b]);
    {
      // This loop will be vectorized on the VE as vector compress instruction (`vcp`)
      size_t pos = 0;
      #pragma _NEC vector
      for (auto i = 0; i < bucket_assignments.size(); i++) {
        if (bucket_assignments[i] == b) {
          selected_ids[pos++] = i;
        }
      }
    }

    // Create a filtered copy based on the list of indexes
    output[b] = this->select(selected_ids);
  }

  return output;
}

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::merge(const NullableScalarVec<T> * const * const inputs,
                                                   const size_t batches) {
  // Count the total number of elements
  size_t rows = 0;
  #pragma _NEC vector
  for (auto b = 0; b < batches; b++) {
    rows += inputs[b]->count;
  }

  // Allocate
  auto *output = allocate();

  // Set the total count, and allocate data and validityBuffer
  output->count = rows;
  output->data = static_cast<T *>(malloc(sizeof(T) * rows));
  if (output->data == nullptr) {
    std::cout << "NullableScalarVec<T>::merge() (output) failed." << std::endl;
  }
  output->validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * frovedis::ceil_div(rows, size_t(64)), 1));
  if (output->validityBuffer == nullptr) {
    std::cout << "NullableScalarVec<T>::merge() (validity) failed." << std::endl;
  }

  // Copy the data and preserve the validityBuffer across the merge
  auto o = 0;
  #pragma _NEC ivdep
  for (auto b = 0; b < batches; b++) {
    for (auto i = 0; i < inputs[b]->count; i++) {
      output->data[o] = inputs[b]->data[i];
      output->set_validity(o++, inputs[b]->get_validity(i));
    }
  }

  return output;
}

template <typename T>
const std::vector<size_t> NullableScalarVec<T>::eval_in(const std::vector<T> &elements) const {
  std::vector<size_t> bitmask(count);

  // Loop over the IN elements first (makes the code more vectorizable)
  #pragma _NEC ivdep
  for (auto j = 0; j < elements.size(); j++) {
    // Loop over column values
    for (auto i = 0; i < bitmask.size(); i++) {
      // Apply ||= to `(element value == column value)`
      bitmask[i] = (bitmask[i] || data[i] == elements[j]);
    }
  }

  return bitmask;
}

// Forward declare the class template instantiations to prevent linker errors:
// https://stackoverflow.com/questions/3008541/template-class-symbols-not-found
template class NullableScalarVec<int32_t>;
template class NullableScalarVec<int64_t>;
template class NullableScalarVec<float>;
template class NullableScalarVec<double>;
