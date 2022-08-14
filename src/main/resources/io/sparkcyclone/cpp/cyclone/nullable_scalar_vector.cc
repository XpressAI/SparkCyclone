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
#include "cyclone/algorithm/bitset.hpp"
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
  // Resize to the desired size
  resize(src.size());

  // Copy the data
  for (auto i = 0; i < src.size(); i++) {
    data[i] = src[i];
  }

  // Set the validityBuffer
  const size_t vcount = ceil(src.size() / 64.0);
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

  // Set the validityBuffer
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

  // Initialize new data buffer
  data = static_cast<T *>(malloc(sizeof(T) * count));
  if (data == nullptr) {
    std::cerr << "NullableScalarVec<T>::resize() (data) failed." << std::endl;
  }

  // Initialize new validityBuffer (set to 8-byte boundary size for Arrow compatibility)
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

  // Allocate validityBuffer in 8-byte sizes for Arrow compatibility
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

  // Set the total count
  output->count = rows;

  // Allocate data
  output->data = static_cast<T *>(malloc(sizeof(T) * rows));
  if (output->data == nullptr) {
    std::cerr << "NullableScalarVec<T>::merge() (output) failed." << std::endl;
  }

  // Allocate the validityBuffer
  output->validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * frovedis::ceil_div(rows, size_t(64)), 1));
  if (output->validityBuffer == nullptr) {
    std::cerr << "NullableScalarVec<T>::merge() (validity) failed." << std::endl;
  }

  // Copy the data
  auto o = 0;
  #pragma _NEC ivdep
  for (auto b = 0; b < batches; b++) {
    for (auto i = 0; i < inputs[b]->count; i++) {
      output->data[o++] = inputs[b]->data[i];
    }
  }

  // Preserve the validityBuffer across the merge
  cyclone::bitset::fast_validity_merge(output->validityBuffer, inputs, batches);

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

template<typename T>
void NullableScalarVec<T>::group_indexes_on_subset(const size_t * input_index_arr,
                                                   const size_t * input_group_delims_arr,
                                                   const size_t   input_group_delims_len,
                                                   size_t       * output_index_arr,
                                                   size_t       * output_group_delims_arr,
                                                   size_t       & output_group_delims_len) const {
  // If there are more group positions than elements in the vector, it means
  // each subset will contain just one element, so we can apply shortcut.
  if (input_group_delims_len > count) {
    auto start = input_group_delims_arr[0];
    auto end = input_group_delims_arr[input_group_delims_len - 1];

    // If iteration order array is not given, generate start.to(end)
    if (input_index_arr == nullptr) {
      #pragma _NEC vector
      #pragma _NEC ivdep
      for (auto i = start; i < end; i++) {
        output_index_arr[i] = i;
      }
    } else {
      // Else just copy the index array to the output
      memcpy(&output_index_arr[start], &input_index_arr[start], sizeof(size_t) * (end - start));
    }

    // Copy the group positions to output
    memcpy(output_group_delims_arr, input_group_delims_arr, sizeof(size_t) * input_group_delims_len);
    output_group_delims_len = input_group_delims_len;
    return;
  }

  // Figure out the largest possible group (subset of data) and allocate memory
  // of that size.  This chunk of memory will be used for sorting each subset
  // so that we only need to allocate and free once.
  size_t largest_group_size = 0;
  #pragma _NEC vector
  for (auto g = 1; g < input_group_delims_len; g++) {
    auto element_count = input_group_delims_arr[g] - input_group_delims_arr[g - 1];
    if (largest_group_size < element_count) largest_group_size = element_count;
  }
  T * sorted_data = static_cast<T *>(malloc(sizeof(T) * largest_group_size));

  // Initialize the output groups
  output_group_delims_len = 0;
  output_group_delims_arr[output_group_delims_len++] = input_group_delims_arr[0];

  // Iterate over all subsets
  #pragma _NEC vector
  for (auto g = 1; g < input_group_delims_len; g++) {
    auto start = input_group_delims_arr[g - 1];
    auto end = input_group_delims_arr[g];
    auto element_count = end - start;

    // If there is only one element in the current subset, then we're done with
    // its grouping
    if (element_count == 1) {
      if (input_index_arr == nullptr) {
        output_index_arr[start] = start;
      } else {
        output_index_arr[start] = input_index_arr[start];
      }
      output_group_delims_arr[output_group_delims_len++] = end;

    } else {
      // Hold the count of valid and invalid elements in the subset
      size_t cur_invalid_count = 0;
      size_t cur_valid_count = 0;

      // Shift indices of all valid and invalid elements to the left and ight of
      // the subset, respectively
      {
        if (input_index_arr == nullptr) {
          #pragma _NEC vector
          #pragma _NEC ivdep
          for (auto i = start; i < end; i++) {
            if (get_validity(i)) {
              output_index_arr[start + cur_valid_count++] = i;
            } else {
              output_index_arr[end - (++cur_invalid_count)] = i;
            }
          }

        } else {
          #pragma _NEC vector
          #pragma _NEC ivdep
          for (auto i = start; i < end; i++) {
            auto j = input_index_arr[i];
            if (get_validity(j)) {
              output_index_arr[start + cur_valid_count++] = j;
            } else {
              output_index_arr[end - (++cur_invalid_count)] = j;
            }
          }
        }
      }


      {
        // Set up the data array for the subset
        #pragma _NEC vector
        for (auto i = 0; i < cur_valid_count; i++) {
          sorted_data[i] = data[output_index_arr[start + i]];
        }
      }

      // Grouping data using Frovedis involves sorting, followed by set_separate
      // to figure out the indices in the sorted array where the element changes
      // value.
      frovedis::radix_sort(sorted_data, &output_index_arr[start], cur_valid_count);
      std::vector <size_t> input_group_delims_arr_idxs = frovedis::set_separate(sorted_data, cur_valid_count);

      auto input_group_delims_arr_idxs_arr = input_group_delims_arr_idxs.data();
      auto out_group_idx = output_group_delims_len;

      // Copy the subset's grouping indices to the output
      #pragma _NEC vector
      for (auto i = 1; i < input_group_delims_arr_idxs.size(); i++) {
        // We are skipping the first entry here, because it will already be
        // included in the result, either as the very first value, or because
        // it was specified as the last value from a previous iteration.
        auto offset_idx = input_group_delims_arr_idxs_arr[i] + start;
        output_group_delims_arr[out_group_idx++] = offset_idx;
      }

      output_group_delims_len = out_group_idx;

      // The last group index will be based on the last valid group
      // to account for the invalid group, if it exists, we need to
      // add the last possible index of this subset, too
      if (cur_invalid_count > 0) {
        output_group_delims_arr[output_group_delims_len++] = end;
      }
    }
  }

  free(sorted_data);
}

template <typename T>
const std::vector<std::vector<size_t>> NullableScalarVec<T>::group_indexes() const {
  // Short-circuit for simple cases
  if (count == 0) return {};
  if (count == 1) return {{ 0 }};

  // Construct the memory buffer args using std::vector for RAII advantage
  size_t input_group_delims[2] = { 0, static_cast<size_t>(this->count) };
  std::vector<size_t> output_index(this->count);
  std::vector<size_t> output_group_delims(this->count + 1);
  size_t output_group_delims_len;

  // group_indexes() is a special case of group_indexes_on_subset(), where we
  // are performing sort + grouping on just one subset - the entire data array.
  group_indexes_on_subset(
    nullptr,
    input_group_delims,
    2,
    output_index.data(),
    output_group_delims.data(),
    output_group_delims_len
  );

  std::vector<std::vector<size_t>> result;
  #pragma _NEC vector
  for (auto g = 1; g < output_group_delims_len; g++) {
    result.emplace_back(std::vector<size_t>(&output_index[output_group_delims[g - 1]], &output_index[output_group_delims[g]]));
  }

  return result;
}

// Forward declare the class template instantiations to prevent linker errors:
// https://stackoverflow.com/questions/3008541/template-class-symbols-not-found
template class NullableScalarVec<int32_t>;
template class NullableScalarVec<int64_t>;
template class NullableScalarVec<float>;
template class NullableScalarVec<double>;
