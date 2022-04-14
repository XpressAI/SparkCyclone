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
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include <stdlib.h>
#include <iostream>

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::allocate() {
  // Allocate
  auto *output = static_cast<NullableScalarVec<T> *>(malloc(sizeof(NullableScalarVec<T>)));
  if (output == nullptr) {
    std::cerr << "NullableScalarVec<T>::allocate() failed." << std::endl;
  }

  // Initialize
  return new (output) NullableScalarVec<T>;
}

template <typename T>
NullableScalarVec<T> * NullableScalarVec<T>::constant(const size_t size, const T value) {
  // Allocate
  auto *output = static_cast<NullableScalarVec<T> *>(malloc(sizeof(NullableScalarVec<T>)));
  // Initialize
  return new (output) NullableScalarVec<T>(size, value);
}

template <typename T>
NullableScalarVec<T>::NullableScalarVec(const std::vector<T> &src) {
  // Initialize count
  count = src.size();

  // Copy the data
  data = static_cast<T *>(malloc(sizeof(T) * src.size()));
  if (data == nullptr) {
    std::cerr << "NullableScalarVec<T>::__ctor() (data) failed." << std::endl;
  }
  for (auto i = 0; i < src.size(); i++) {
    data[i] = src[i];
  }

  // Set the validityBuffer
  const size_t vcount = ceil(src.size() / 64.0);
  validityBuffer = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * vcount));
  if (validityBuffer == nullptr) {
    std::cerr << "NullableScalarVec<T>::__ctor() (validity) failed." << std::endl;
  }
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

template <typename T>
NullableScalarVec<T>::NullableScalarVec(const size_t size, const T value) {
  // Resize to the desired size
  resize(size);

  // Populate with the same value
  #pragma _NEC vector
  for (auto i = 0; i < size; i++) {
    data[i] = value;
  }

  const size_t vcount = ceil(size / 64.0);
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
    std::cerr << "NullableScalarVec<T>::resize() (data) failed." << std::endl;
  }

  // Set validityBuffer to new buffer
  const auto vbytes = frovedis::ceil_div(count, int32_t(64)) * sizeof(uint64_t);
  validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  if (validityBuffer == nullptr) {
    std::cerr << "NullableScalarVec<T>::__ctor() (validity) failed." << std::endl;
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
const std::vector<size_t> NullableScalarVec<T>::size_t_validity_vec() const {
  std::vector<size_t> bitmask(count);

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
  const auto dbytes = output->count * sizeof(T);
  output->data = static_cast<T *>(malloc(dbytes));
  if (output->data == nullptr) {
    std::cerr << "NullableScalarVec<T>::clone() (data) failed." << std::endl;
  }
  memcpy(output->data, data, dbytes);

  // Copy the validity buffer
  const auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  if (output->data == nullptr) {
    std::cerr << "NullableScalarVec<T>::clone() (validity) failed." << std::endl;
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
  const auto dbytes = output->count * sizeof(T);
  output->data = static_cast<T *>(malloc(dbytes));
  if (output->data == nullptr) {
    std::cerr << "NullableScalarVec<T>::select() (data) failed." << std::endl;
  }

  // Allocate validityBuffer
  const auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  if (output->validityBuffer == nullptr) {
    std::cerr << "NullableScalarVec<T>::select() (validity) failed." << std::endl;
  }

  // Preserve the validityBuffer across the select
  #pragma _NEC vector
  for (auto o = 0; o < selected_ids.size(); o++) {
    // Fetch the original index
    const int i = selected_ids[o];

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
    std::cerr << "NullableScalarVec<T>::bucket() (output) failed." << std::endl;
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
    std::cerr << "NullableScalarVec<T>::merge() (output) failed." << std::endl;
  }
  output->validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * frovedis::ceil_div(rows, size_t(64)), 1));
  if (output->validityBuffer == nullptr) {
    std::cerr << "NullableScalarVec<T>::merge() (validity) failed." << std::endl;
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

template <typename T>
const std::vector<std::vector<size_t>> NullableScalarVec<T>::group_indexes() const {
  // Check if validity needs to be checked during grouping
  bool all_valid = true;
  const size_t vcount = ceil(count / 64.0);
  #pragma _NEC vector
  for (auto i = 0; i < vcount; ++i) {
    if(validityBuffer[i] != 0xffffffffffffffff){
      all_valid = false;
      break;
    }
  }

  if(all_valid){
    // Fast path: No validity checking needed

    // Copy data for sorting
    T* sorted_data = static_cast<T *>(malloc(sizeof(T) * count));
    #pragma _NEC vector
    for(auto i = 0; i < count; ++i){
      sorted_data[i] = data[i];
    }

    // Sort data for grouping
    frovedis::radix_sort(sorted_data, count);

    // Get unique groups
    std::vector<T> unique_groups = frovedis::set_unique(sorted_data, count);
    auto unique_group_arr = unique_groups.data();
    auto unique_group_count = unique_groups.size();

    // clean up sorted_data
    free(sorted_data);

    // Calculate size of each group
    size_t* group_sizes = static_cast<size_t *>(malloc(sizeof(size_t) * unique_group_count));

    #pragma _NEC vector
    for(auto g = 0; g < unique_group_count; ++g){
      group_sizes[g] = 0;
      #pragma _NEC vector
      for(auto i = 0; i < count; ++i){
        if(unique_group_arr[g] == data[i]){
          ++group_sizes[g];
        }
      }
    }

    // Setup group indexes in continuous memory
    size_t* group_indexes = static_cast<size_t *>(malloc(sizeof(size_t) * count));

    // Copy starting indexes for every group
    size_t* group_pos = static_cast<size_t *>(malloc(sizeof(size_t) * unique_group_count));
    group_pos[0] = 0;
    #pragma _NEC vector
    for(auto g = 1; g < unique_group_count; ++g) {
      group_pos[g] = group_pos[g-1] + group_sizes[g-1];
    }

    #pragma _NEC vector
    for(auto g = 0; g < unique_group_count; ++g){
      size_t cur_pos = group_pos[g];
      #pragma _NEC vector
      for(auto i = 0; i < count; ++i){
        if(unique_group_arr[g] == data[i]){
          group_indexes[cur_pos++] = i;
        }
      }
    }

    // Reset group starting points
    group_pos[0] = 0;
    #pragma _NEC vector
    for(auto g = 1; g < unique_group_count; ++g) {
      group_pos[g] = group_pos[g-1] + group_sizes[g-1];
    }

    // Setup output
    std::vector<std::vector<size_t>> result(unique_group_count);
    for(auto g = 0; g < unique_group_count; ++g) {
      result[g].resize(group_sizes[g]);

      size_t offset = group_pos[g];
      auto result_g_arr = result[g].data();
      auto result_g_size = result[g].size();
      for(auto i = 0; i < result_g_size; ++i){
        result_g_arr[i]  = group_indexes[i + offset];
      }
    }

    // clean up  group_sizes, group_pos, group_indexes
    free(group_sizes);
    free(group_pos);
    free(group_indexes);

    return result;
  }else{
    // Slower path: Need validity checking

    // Copy data for sorting
    T* sorted_data = static_cast<T *>(malloc(sizeof(T) * count));
    #pragma _NEC vector
    for(auto i = 0; i < count; ++i){
      sorted_data[i] = data[i];
    }

    // Sort data for grouping
    frovedis::radix_sort(sorted_data, count);

    // Get unique groups
    std::vector<T> unique_groups = frovedis::set_unique(sorted_data, count);
    auto unique_group_arr = unique_groups.data();
    auto unique_group_count = unique_groups.size();

    // clean up sorted_data
    free(sorted_data);

    // Calculate size of each group + invalid group
    size_t* group_sizes = static_cast<size_t *>(malloc(sizeof(size_t) * (unique_group_count + 1)));

    // Count valid groups
    #pragma _NEC vector
    for(auto g = 0; g < unique_group_count; ++g){
      group_sizes[g] = 0;
      #pragma _NEC vector
      for(auto i = 0; i < count; ++i){
        if(get_validity(i) && unique_group_arr[g] == data[i]){
          ++group_sizes[g];
        }
      }
    }

    // Count invalid groups
    group_sizes[unique_group_count] = 0;
    for(auto i = 0; i < count; ++i){
      if(!get_validity(i)){
        ++group_sizes[unique_group_count];
      }
    }

    // Setup group indexes in continuous memory
    size_t* group_indexes = static_cast<size_t *>(malloc(sizeof(size_t) * count));

    // Copy starting indexes for every group
    size_t* group_pos = static_cast<size_t *>(malloc(sizeof(size_t) * (unique_group_count + 1)));
    group_pos[0] = 0;
    #pragma _NEC vector
    for(auto g = 1; g < unique_group_count + 1; ++g) {
      group_pos[g] = group_pos[g-1] + group_sizes[g-1];
    }

    // Set valid group indices
    #pragma _NEC vector
    for(auto g = 0; g < unique_group_count; ++g){
      size_t cur_pos = group_pos[g];
      #pragma _NEC vector
      for(auto i = 0; i < count; ++i){
        if(get_validity(i) && unique_group_arr[g] == data[i]){
            group_indexes[cur_pos++] = i;
        }
      }
    }

    { // Set invalid group indices
      auto cur_pos = group_pos[unique_group_count];
      #pragma _NEC vector
      for(auto i = 0; i < count; ++i){
        if(!get_validity(i)){
          group_indexes[cur_pos++] = i;
        }
      }
    }

    // Reset group starting points
    group_pos[0] = 0;
    #pragma _NEC vector
    for(auto g = 1; g < unique_group_count + 1; ++g) {
      group_pos[g] = group_pos[g-1] + group_sizes[g-1];
    }

    // Setup output
    std::vector<std::vector<size_t>> result(unique_group_count + 1);
    for(auto g = 0; g < unique_group_count + 1; ++g) {
      result[g].resize(group_sizes[g]);

      size_t offset = group_pos[g];
      auto result_g_arr = result[g].data();
      auto result_g_size = result[g].size();
      for(auto i = 0; i < result_g_size; ++i){
        result_g_arr[i]  = group_indexes[i + offset];
      }
    }

    // clean up  group_sizes, group_pos, group_indexes
    free(group_sizes);
    free(group_pos);
    free(group_indexes);

    return result;
  }
}

// Forward declare the class template instantiations to prevent linker errors:
// https://stackoverflow.com/questions/3008541/template-class-symbols-not-found
template class NullableScalarVec<int32_t>;
template class NullableScalarVec<int64_t>;
template class NullableScalarVec<float>;
template class NullableScalarVec<double>;
