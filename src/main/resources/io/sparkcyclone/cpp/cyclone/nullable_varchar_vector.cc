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
#include "frovedis/text/char_int_conv.hpp"
#include "frovedis/text/datetime_utility.hpp"
#include "frovedis/text/dict.hpp"
#include "cyclone_utils.hpp"
#include <stdlib.h>
#include <iostream>
#include <cstring>

nullable_varchar_vector * nullable_varchar_vector::allocate() {
  // Allocate
  auto *output = static_cast<nullable_varchar_vector *>(malloc(sizeof(nullable_varchar_vector)));
  // Initialize
  return new (output) nullable_varchar_vector;
}

nullable_varchar_vector * nullable_varchar_vector::constant(const size_t size, const std::string &value) {
  // Allocate
  auto *output = static_cast<nullable_varchar_vector *>(malloc(sizeof(nullable_varchar_vector)));
  // Initialize
  return new (output) nullable_varchar_vector(size, value);
}

nullable_varchar_vector * nullable_varchar_vector::from_words(const frovedis::words &src) {
  // Allocate
  auto *output = allocate();

  // Use placement new to construct nullable_varchar_vector from frovedis::words
  // in the pre-allocated memory
  return new (output) nullable_varchar_vector(src);
}

nullable_varchar_vector::nullable_varchar_vector(const std::vector<std::string> &src) {
  // Initialize count
  count = src.size();

  // Initialize dataSize
  dataSize = 0;
  for (auto i = 0; i < count; i++) {
    dataSize += src[i].size();
  }

  // Initialize data and copy strings to data
  data = static_cast<int32_t *>(malloc(sizeof(int32_t) * dataSize));
  auto p = 0;
  for (auto i = 0; i < count; i++) {
    for (auto j = 0; j < src[i].size(); j++) {
      data[p++] = static_cast<int32_t>(src[i][j]);
    }
  }

  // Initialize and set the lengths
  lengths = static_cast<int32_t *>(calloc(sizeof(int32_t) * count, 1));
  for (auto i = 0; i < count; i++) {
    lengths[i] = src[i].size();
  }

  // Initialize and set the offsets
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * count, 1));
  offsets[0] = 0;
  for (auto i = 1; i < count; i++) {
    offsets[i] = offsets[i-1] + lengths[i-1];
  }

  // Initialize and set the validityBuffer (set to 8-byte boundary size for Arrow compatibility)
  const size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * vcount));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

nullable_varchar_vector::nullable_varchar_vector(const size_t size, const std::string &value) {
  // Initialize count
  count = size;

  // Initialize dataSize
  dataSize = value.size();

  // Initialize data and copy string to data
  data = static_cast<int32_t *>(malloc(sizeof(int32_t) * dataSize));
  for (auto i = 0; i < value.size(); i++) {
    data[i] = static_cast<int32_t>(value[i]);
  }

  // Initialize and set the lengths
  lengths = static_cast<int32_t *>(calloc(sizeof(int32_t) * count, 1));
  for (auto i = 0; i < count; i++) {
    lengths[i] = value.size();
  }

  // Initialize and set the offsets to zero
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * count, 1));

  // Initialize and set the validityBuffer
  size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * vcount));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

nullable_varchar_vector::nullable_varchar_vector(const frovedis::words &src) {
  // Set count
  count = src.lens.size();

  // Set dataSize
  dataSize = src.chars.size();

  // Copy chars to data
  data = static_cast<int32_t *>(malloc(sizeof(int32_t) * dataSize));
  std::copy(src.chars.begin(), src.chars.end(), data);

  // Set the offsets
  lengths = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.starts.size()), 1));
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.starts.size()), 1));

  for (auto i = 0; i < src.starts.size(); i++) {
    offsets[i] = src.starts[i];
    lengths[i] = src.lens[i];
  }

  // Initialize and set the validityBuffer
  const size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(malloc(sizeof(uint64_t) * vcount));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

void nullable_varchar_vector::reset() {
  // Free the owned memory
  free(data);
  free(offsets);
  free(lengths);
  free(validityBuffer);

  // Reset the pointers and values
  data            = nullptr;
  offsets         = nullptr;
  validityBuffer  = nullptr;
  lengths         = nullptr;
  dataSize        = 0;
  count           = 0;
}

void nullable_varchar_vector::move_assign_from(nullable_varchar_vector * other) {
  // Reset the pointers and values
  reset();

  // Assign the pointers and values from other
  data            = other->data;
  offsets         = other->offsets;
  validityBuffer  = other->validityBuffer;
  dataSize        = other->dataSize;
  lengths         = other->lengths;
  count           = other->count;

  // Free the other (struct only)
  free(other);
}

bool nullable_varchar_vector::is_default() const {
  return data == nullptr &&
    offsets == nullptr &&
    validityBuffer  == nullptr &&
    dataSize == 0 &&
    count == 0;
}

frovedis::words nullable_varchar_vector::to_words() const {
  frovedis::words output;
  if (count == 0) {
    return output;
  }

  // Set the lens
  output.lens.resize(count);
  for (auto i = 0; i < count; i++) {
    output.lens[i] = lengths[i];
  }

  // Set the starts
  output.starts.resize(count);
  for (auto i = 0; i < count; i++) {
    output.starts[i] = offsets[i];
  }

  // Set the chars
  output.chars.assign(data, data + dataSize);
  return output;
}

void nullable_varchar_vector::print() const {
  std::stringstream stream;
  stream << "nullable_varchar_vector @ " << this << " {\n";

  // Print count
  stream << "  COUNT: " << count << "\n";

  // Print dataSize
  stream << "  DATA SIZE: " << dataSize << "\n";

  if (count <= 0) {
    stream << "  VALUES: [ ]\n"
           << "  OFFSETS: [ ]\n"
           << "  VALIDITY: [ ]\n"
           << "  LENGTHS: [ ]\n"
           << "  DATA: [ ]\n";
  } else {
    // Print string values
    stream << "  VALUES: [ ";
    for (auto i = 0; i < count; i++) {
      if (get_validity(i)) {
        for (auto j = offsets[i]; j < offsets[i] + lengths[i]; j++) {
          stream << char(data[j]);
        }
        stream << ", ";
      } else {
        stream << "#, ";
      }
    }

    // Print offsets
    stream << "]\n  OFFSETS: [";
    for (auto i = 0; i < count; i++) {
      stream << offsets[i] << ", ";
    }

    // Print lengths
    stream << "]\n  LENGTHS: [";
    for (auto i = 0; i < count; i++) {
      stream << lengths[i] << ", ";
    }

    // Print validityBuffer
    stream << "]\n  VALIDITY: [";
    for (auto i = 0; i < count; i++) {
      stream << get_validity(i) << ", ";
    }

    // Print data
    stream << "]\n  DATA: [";
    for (auto i = 0; i < dataSize; i++) {
      stream << char(data[i]);
    }

    stream << "]\n";
  }

  stream << "}\n";
  std::cout << stream.str() << std::endl;
}

bool nullable_varchar_vector::equals(const nullable_varchar_vector * const other) const {
  if (is_default() && other->is_default()) {
    return true;
  }

  // Compare count
  auto output = (count == other->count);

  // Compare dataSize
  output = output && (dataSize == other->dataSize);

  // Compare data
  #pragma _NEC ivdep
  for (auto i = 0; i < dataSize; i++) {
    output = output && (data[i] == other->data[i]);
  }

  // Compare offsets
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (offsets[i] == other->offsets[i]);
  }

  // Compare lengths
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (lengths[i] == other->lengths[i]);
  }

  // Compare validityBuffer
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (get_validity(i) == other->get_validity(i));
  }

  return output;
}

bool nullable_varchar_vector::equivalent_to(const nullable_varchar_vector * const other) const {
  if (is_default() && other->is_default()) {
    return true;
  }

  // Compare count
  auto output = (count == other->count);

  // Compare lengths
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (lengths[i] == other->lengths[i]);
  }

  // Compare data (the physical data buffer might be different, but equivalent data should be encoded)
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    for (auto j = 0; j < lengths[i];  j++) {
      output = output && (data[offsets[i] + j] == other->data[other->offsets[i] + j]);
    }
  }

  // Compare validityBuffer
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (get_validity(i) == other->get_validity(i));
  }

  return output;
}

nullable_varchar_vector * nullable_varchar_vector::clone() const {
  // Allocate the output
  auto *output = allocate();

  // Copy the count and dataSizes
  output->count = count;
  output->dataSize = dataSize;

  // Copy the data
  auto dbytes = output->dataSize * sizeof(int32_t);
  output->data = static_cast<int32_t *>(malloc(dbytes));
  memcpy(output->data, data, dbytes);

  // Copy the offsets
  auto obytes = (output->count) * sizeof(int32_t);
  output->offsets = static_cast<int32_t *>(malloc(obytes));
  memcpy(output->offsets, offsets, obytes);

  // Copy the lengths
  auto lbytes = (output->count) * sizeof(int32_t);
  output->lengths = static_cast<int32_t *>(malloc(lbytes));
  memcpy(output->lengths, lengths, lbytes);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

nullable_varchar_vector * nullable_varchar_vector::select(const std::vector<size_t> &selected_ids) const {
  // Get the frovedis::words representation of the input
  auto input_words = to_words();

  // Initialize the starts and lens
  std::vector<size_t> starts(selected_ids.size());
  std::vector<size_t> lens(selected_ids.size());

  #pragma _NEC vector
  for (auto g = 0; g < selected_ids.size(); g++) {
    // Fetch the original index
    const auto i = selected_ids[g];

    // Copy the start and len values
    starts[g] = input_words.starts[i];
    lens[g] = input_words.lens[i];
  }

  // Use starts, lens, and frovedis::concat_words to generate the frovedis::words
  // version of the selected nullable_varchar_vector
  std::vector<size_t> new_starts;
  std::vector<int> new_chars = frovedis::concat_words(
    input_words.chars,
    (const std::vector<size_t>&)(starts),
    (const std::vector<size_t>&)(lens),
    "",
    (std::vector<size_t>&)(new_starts)
  );
  input_words.chars = new_chars;
  input_words.starts = new_starts;
  input_words.lens = lens;

  // Map the data from frovedis::words back to nullable_varchar_vector
  auto *output = from_words(input_words);

  // Preserve the validityBuffer across the select
  #pragma _NEC vector
  for (auto g = 0; g < selected_ids.size(); g++) {
    // Fetch the original index
    const int i = selected_ids[g];

    // Copy the validity buffer
    output->set_validity(g, get_validity(i));
  }

  return output;
}

nullable_varchar_vector ** nullable_varchar_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_varchar_vector pointers
  auto ** output = static_cast<nullable_varchar_vector **>(malloc(sizeof(nullable_varchar_vector *) * bucket_counts.size()));

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

const std::vector<int64_t> nullable_varchar_vector::hash_vec() const {
  // Allocate vec
  std::vector<int64_t> output(count);

  // Assign the hash of each string in the nullable_varchar_vector
  #pragma _NEC vector
  for (auto i = 0; i < count; i++) {
    output[i] = hash_at(i, 1);
  }

  return output;
}

const std::vector<int32_t> nullable_varchar_vector::validity_vec() const {
  std::vector<int32_t> bitmask(count);

  #pragma _NEC vector
  for (auto i = 0; i < count; i++) {
    bitmask[i] = get_validity(i);
  }

  return bitmask;
}

nullable_varchar_vector * nullable_varchar_vector::merge(const nullable_varchar_vector * const * const inputs,
                                                         const size_t batches) {

  // Construct std::vector<frovedis::words> from the inputs
  std::vector<frovedis::words> multi_words(batches);
  #pragma _NEC vector
  for (auto b = 0; b < batches; b++) {
    multi_words[b] = inputs[b]->to_words();
  }

  // Merge using Frovedis and convert back to nullable_varchar_vector
  auto *output = from_words(frovedis::merge_multi_words(multi_words));

 // Preserve the validityBuffer across the merge
  fast_validity_merge(output->validityBuffer, inputs, batches);
  return output;
}

const std::vector<int32_t> nullable_varchar_vector::date_cast() const {
  static const auto epoch = frovedis::makedatetime(1970, 1, 1, 0, 0, 0, 0);

  const auto words = to_words();
  const auto datetimes = frovedis::parsedatetime(words, std::string("%Y-%m-%d"));

  std::vector<int32_t> dates(count);
  #pragma _NEC vector
  for (auto i = 0; i < count; i++) {
    dates[i] = frovedis::datetime_diff_day(datetimes[i], epoch);
  }

  return dates;
}

const std::vector<size_t> nullable_varchar_vector::eval_like(const std::string &pattern) const {
  const auto words = to_words();
  const auto matching_ids = frovedis::like(words, pattern);

  std::vector<size_t> bitmask(count);
  #pragma _NEC vector
  for (auto i = 0; i < matching_ids.size(); i++) {
    bitmask[matching_ids[i]] = 1;
  }

  return bitmask;
}

const std::vector<size_t> nullable_varchar_vector::eval_in(const frovedis::words &elements) const {
  const auto compressed_words = frovedis::make_compressed_words(to_words());
  const auto dct = frovedis::make_dict_from_words(elements);
  const auto find_results = dct.lookup(compressed_words);

  std::vector<size_t> bitmask(count);
  #pragma _NEC vector
  for (auto i = 0; i < bitmask.size(); i++) {
    bitmask[i] = (find_results[i] != std::numeric_limits<size_t>::max());
  }

  return bitmask;
}

const std::vector<size_t> nullable_varchar_vector::eval_in(const std::vector<std::string> &elements) const {
  const nullable_varchar_vector tmp(elements);
  return eval_in(tmp.to_words());
}

nullable_varchar_vector * nullable_varchar_vector::from_binary_choice(const size_t count,
                                                                      const cyclone::function_view<bool(size_t)> &condition,
                                                                      const std::string &trueval,
                                                                      const std::string &falseval) {
  // Create int vectors for both the true and false cases
  std::vector<int32_t> output_chars = frovedis::char_to_int(trueval);
  std::vector<int32_t> false_chars = frovedis::char_to_int(falseval);

  // Set the positions and lengths
  const int32_t true_pos = 0;
  const int32_t true_len = output_chars.size();
  const int32_t false_pos = output_chars.size();
  const int32_t false_len = false_chars.size();

  // Combine to single vector
  output_chars.insert(output_chars.end(), false_chars.begin(), false_chars.end());

  // Prepare the output starts and lens vectors
  std::vector<size_t> output_starts(count);
  std::vector<size_t> output_lens(count);

  #pragma _NEC vector
  for (auto i = 0; i < count; i++) {
    // If condition is true, set to the true case, else set to the false case
    if (condition(i)) {
      output_starts[i] = true_pos;
      output_lens[i] = true_len;
    } else {
      output_starts[i] = false_pos;
      output_lens[i] = false_len;
    }
  }

  // Set the output words
  frovedis::words output_words;
  output_words.chars.swap(output_chars);
  output_words.starts.swap(output_starts);
  output_words.lens.swap(output_lens);

  // Convert to nullable_varchar_vector
  return from_words(output_words);
}

void nullable_varchar_vector::group_indexes_on_subset(size_t* iter_order_arr, size_t* group_pos, size_t group_pos_size, size_t* idx_arr, size_t* out_group_pos, size_t &out_group_pos_size) const {
  // Shortcut for case when every element would end up in its own group anyway
  if(group_pos_size > count){
    auto start = group_pos[0];
    auto end = group_pos[group_pos_size - 1];
    auto count = end - start;

    if(iter_order_arr == nullptr){
      #pragma _NEC vector
      #pragma _NEC ivdep
      for(auto i = start; i < end; i++) {
          idx_arr[i] = i;
      }
    } else {
      memcpy(&idx_arr[start], &iter_order_arr[start], sizeof(size_t) * count);
    }
    memcpy(out_group_pos, group_pos, sizeof(size_t) * group_pos_size);
    out_group_pos_size = group_pos_size;
    return;
  }

  // Allocate memory for the largest possible group
  // We will reuse that memory for every group, so we don't have to allocate/free often
  size_t largest_group_size = 0;
#pragma _NEC vector
  for(auto g = 1; g < group_pos_size; g++){
    auto total_el_count = group_pos[g] - group_pos[g - 1];
    if(largest_group_size < total_el_count) largest_group_size = total_el_count;
  }
  size_t* sorted_data = static_cast<size_t *>(malloc(sizeof(size_t) * largest_group_size));

  out_group_pos_size = 0;
  out_group_pos[out_group_pos_size++] = group_pos[0];
#pragma _NEC vector
  for(auto g = 1; g < group_pos_size; g++){
    auto start = group_pos[g - 1];
    auto end = group_pos[g];

    auto total_el_count = end - start;
    if(total_el_count == 1){
      // Shortcut for single element groups
      if(iter_order_arr == nullptr){
        idx_arr[start] = start;
      }else{
        idx_arr[start] = iter_order_arr[start];
      }
      out_group_pos[out_group_pos_size++] = end;
    }else{
      size_t cur_invalid_count = 0;
      size_t cur_valid_count = 0;

      if(iter_order_arr == nullptr){
#pragma _NEC vector
#pragma _NEC ivdep
        for(auto i = start; i < end; i++){
          if(get_validity(i)){
            idx_arr[start + cur_valid_count++] = i;
          }else{
            idx_arr[end - (++cur_invalid_count)] = i;
          }
        }
      }else{
#pragma _NEC vector
#pragma _NEC ivdep
        for(auto i = start; i < end; i++){
          auto j = iter_order_arr[i];
          if(get_validity(j)){
            idx_arr[start + cur_valid_count++] = j;
          }else{
            idx_arr[end - (++cur_invalid_count)] = j;
          }
        }
      }

      { // Setup valid inputs
#pragma _NEC vector
        for (auto i = 0; i < cur_valid_count; i++) {
            sorted_data[i] = hash_at(idx_arr[start + i], 1);
        }
      }

      // Sort data for grouping
      frovedis::radix_sort(sorted_data, &idx_arr[start], cur_valid_count);
      std::vector<size_t> group_pos_idxs = frovedis::set_separate(sorted_data, cur_valid_count);

      auto new_group_count = group_pos_idxs.size();
      auto new_group_arr = group_pos_idxs.data();
      size_t out_group_idx = out_group_pos_size;
#pragma _NEC vector
      for(auto i = 1; i < new_group_count; i++){
        // We are skipping the first entry here, because it will already be
        // included in the result, either as the very first value, or because
        // it was specified as the last value from a previous iteration.
        auto offset_idx = new_group_arr[i] + start;
        out_group_pos[out_group_idx++] = offset_idx;
      }
      out_group_pos_size = out_group_idx;
      // The last group index will be based on the last valid group
      // to account for the invalid group, if it exists, we need to
      // add the last possible index of this subset, too
      if(cur_invalid_count > 0){
        out_group_pos[out_group_pos_size++] = end;
      }
    }
  }

  free(sorted_data);
}

const std::vector<std::vector<size_t>> nullable_varchar_vector::group_indexes() const {
  // Short-circuit for simple cases
  if(count == 0) return {};
  if(count == 1) return {{0}};


  size_t group_pos[2] = {0, static_cast<size_t>(count)};
  size_t* idx_arr = static_cast<size_t *>(malloc(sizeof(size_t) * count));
  size_t* max_group_pos = static_cast<size_t *>(malloc(sizeof(size_t) * (count + 1)));
  size_t group_pos_count;
  group_indexes_on_subset(nullptr, group_pos, 2, idx_arr, max_group_pos, group_pos_count);

  std::vector<std::vector<size_t>> result;

#pragma _NEC vector
  for(auto g = 1; g < group_pos_count; g++){
    std::vector<size_t> output_group(&idx_arr[max_group_pos[g - 1]], &idx_arr[max_group_pos[g]]);
    result.push_back(output_group);
  }

  free(idx_arr);
  free(max_group_pos);

  return result;
}
