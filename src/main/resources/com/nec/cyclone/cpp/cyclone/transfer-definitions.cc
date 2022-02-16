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

inline void nullable_int_vector::set_validity(const size_t idx,
                           const int32_t validity) {
    set_valid_bit(validityBuffer, idx, validity);
  }

inline uint32_t nullable_int_vector::get_validity(const size_t idx) const {
    return get_valid_bit(validityBuffer, idx);
  }

nullable_int_vector::nullable_int_vector(const frovedis::words &src) {
  // Set count
  count = src.lens.size();

  // Set dataSize
  auto total_chars = 0;
  for (size_t i = 0; i < src.lens.size(); i++) {
    total_chars += src.lens[i];
  }
  dataSize = total_chars;

  // Compute last_chars
  std::vector<size_t> last_chars(src.lens.size() + 1);
  auto sum = 0;
  for (auto i = 0; i < src.lens.size(); i++) {
    last_chars[i] = sum;
    sum += src.lens[i];
  }

  // Copy chars to data
  data = static_cast<int *>(malloc(sizeof(int) * total_chars));
  for (auto i = 0; i < count; i++) {
    auto pos = last_chars[i];
    auto word_start = src.starts[i];
    auto word_end = word_start + src.lens[i];
    for (int j = word_start; j < word_end; j++) {
      data[pos++] = (int)src.chars[j];
    }
  }

  // Set the offsets
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.starts.size() + 1), 1));
  for (auto i = 1; i < src.starts.size() + 1; i++) {
    offsets[i] = last_chars[i];
  }
  offsets[src.starts.size()] = total_chars;

  // Set the validityBuffer
  size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * vcount, 1));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

bool nullable_int_vector::equals(const nullable_int_vector * const other) {
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

nullable_int_vector * nullable_int_vector::clone() {
  // Allocate
  auto * output = static_cast<nullable_int_vector *>(malloc(sizeof(nullable_int_vector)));

  // Copy the count
  output->count = count;

  // Copy the data
  output->data = static_cast<int32_t *>(malloc(output->count*4));
  memcpy(output->data, data, output->count*4);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

nullable_int_vector * nullable_int_vector::from_words(const frovedis::words &src) const {
  // Allocate
  auto * output = static_cast<nullable_int_vector *>(malloc(sizeof(nullable_int_vector)));

  // Use placement new to construct nullable_int_vector from frovedis::words
  // in the pre-allocated memory
  return new (output) nullable_int_vector(src);
}

frovedis::words nullable_int_vector::to_words() const {
  frovedis::words output;
  if (count == 0) {
    return output;
  }

  // Set the lens
  output.lens.resize(count);
  for (auto i = 0; i < count; i++) {
    output.lens[i] = offsets[i + 1] - offsets[i];
  }

  // Set the starts
  output.starts.resize(count);
  for (int i = 0; i < count; i++) {
    output.starts[i] = offsets[i];
  }

  // Set the chars
  output.chars.resize(offsets[count]);
  //FIXME
//  frovedis::char_to_int(data, offsets[count], output.chars.data());

  return output;
}

nullable_int_vector * nullable_int_vector::filter(const std::vector<size_t> &matching_ids) const {
  // Get the frovedis::words representation of the input
  auto input_words = to_words();

  // Initialize the starts and lens
  std::vector<size_t> starts(matching_ids.size());
  std::vector<size_t> lens(matching_ids.size());

  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the start and len values
    starts[g] = input_words.starts[i];
    lens[g] = input_words.lens[i];
  }

  // Use starts, lens, and frovedis::concat_words to generate the frovedis::words
  // version of the filtered nullable_int_vector
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

  // Map the data from frovedis::words back to nullable_int_vector
  auto * output = nullable_int_vector::from_words(input_words);

  // Preserve the validityBuffer across the filter
  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the validity buffer
    output->set_validity(g, get_validity(i));
  }

  return output;
}

nullable_int_vector ** nullable_int_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_int_vector pointers
  auto ** output = static_cast<nullable_int_vector **>(malloc(sizeof(nullable_int_vector *) * bucket_counts.size()));

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

inline void nullable_bigint_vector::set_validity(const size_t idx,
                           const int32_t validity) {
    set_valid_bit(validityBuffer, idx, validity);
  }

inline uint32_t nullable_bigint_vector::get_validity(const size_t idx) const {
    return get_valid_bit(validityBuffer, idx);
  }

nullable_bigint_vector::nullable_bigint_vector(const frovedis::words &src) {
  // Set count
  count = src.lens.size();

  // Set dataSize
  auto total_chars = 0;
  for (size_t i = 0; i < src.lens.size(); i++) {
    total_chars += src.lens[i];
  }
  dataSize = total_chars;

  // Compute last_chars
  std::vector<size_t> last_chars(src.lens.size() + 1);
  auto sum = 0;
  for (auto i = 0; i < src.lens.size(); i++) {
    last_chars[i] = sum;
    sum += src.lens[i];
  }

  // Copy chars to data
  data = static_cast<int64_t *>(malloc(sizeof(int64_t) * total_chars));
  for (auto i = 0; i < count; i++) {
    auto pos = last_chars[i];
    auto word_start = src.starts[i];
    auto word_end = word_start + src.lens[i];
    for (int j = word_start; j < word_end; j++) {
      data[pos++] = (int64_t)src.chars[j];
    }
  }

  // Set the offsets
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.starts.size() + 1), 1));
  for (auto i = 1; i < src.starts.size() + 1; i++) {
    offsets[i] = last_chars[i];
  }
  offsets[src.starts.size()] = total_chars;

  // Set the validityBuffer
  size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * vcount, 1));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

bool nullable_bigint_vector::equals(const nullable_bigint_vector * const other) {
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

nullable_bigint_vector * nullable_bigint_vector::clone() {
  // Allocate
  auto * output = static_cast<nullable_bigint_vector *>(malloc(sizeof(nullable_bigint_vector)));

  // Copy the count
  output->count = count;

  // Copy the data
  output->data = static_cast<int64_t *>(malloc(output->count*8));
  memcpy(output->data, data, output->count*8);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

nullable_bigint_vector * nullable_bigint_vector::from_words(const frovedis::words &src) const {
  // Allocate
  auto * output = static_cast<nullable_bigint_vector *>(malloc(sizeof(nullable_bigint_vector)));

  // Use placement new to construct nullable_bigint_vector from frovedis::words
  // in the pre-allocated memory
  return new (output) nullable_bigint_vector(src);
}

frovedis::words nullable_bigint_vector::to_words() const {
  frovedis::words output;
  if (count == 0) {
    return output;
  }

  // Set the lens
  output.lens.resize(count);
  for (auto i = 0; i < count; i++) {
    output.lens[i] = offsets[i + 1] - offsets[i];
  }

  // Set the starts
  output.starts.resize(count);
  for (int i = 0; i < count; i++) {
    output.starts[i] = offsets[i];
  }

  // Set the chars
  output.chars.resize(offsets[count]);
//  frovedis::char_to_int(data, offsets[count], output.chars.data());

  return output;
}

nullable_bigint_vector * nullable_bigint_vector::filter(const std::vector<size_t> &matching_ids) const {
  // Get the frovedis::words representation of the input
  auto input_words = to_words();

  // Initialize the starts and lens
  std::vector<size_t> starts(matching_ids.size());
  std::vector<size_t> lens(matching_ids.size());

  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the start and len values
    starts[g] = input_words.starts[i];
    lens[g] = input_words.lens[i];
  }

  // Use starts, lens, and frovedis::concat_words to generate the frovedis::words
  // version of the filtered nullable_bigint_vector
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

  // Map the data from frovedis::words back to nullable_bigint_vector
  auto * output = nullable_bigint_vector::from_words(input_words);

  // Preserve the validityBuffer across the filter
  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the validity buffer
    output->set_validity(g, get_validity(i));
  }

  return output;
}

nullable_bigint_vector ** nullable_bigint_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_bigint_vector pointers
  auto ** output = static_cast<nullable_bigint_vector **>(malloc(sizeof(nullable_bigint_vector *) * bucket_counts.size()));

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

inline void nullable_float_vector::set_validity(const size_t idx,
                           const int32_t validity) {
    set_valid_bit(validityBuffer, idx, validity);
  }

inline uint32_t nullable_float_vector::get_validity(const size_t idx) const {
    return get_valid_bit(validityBuffer, idx);
  }

nullable_float_vector::nullable_float_vector(const frovedis::words &src) {
  // Set count
  count = src.lens.size();

  // Set dataSize
  auto total_chars = 0;
  for (size_t i = 0; i < src.lens.size(); i++) {
    total_chars += src.lens[i];
  }
  dataSize = total_chars;

  // Compute last_chars
  std::vector<size_t> last_chars(src.lens.size() + 1);
  auto sum = 0;
  for (auto i = 0; i < src.lens.size(); i++) {
    last_chars[i] = sum;
    sum += src.lens[i];
  }

  // Copy chars to data
  data = static_cast<float *>(malloc(sizeof(float) * total_chars));
  for (auto i = 0; i < count; i++) {
    auto pos = last_chars[i];
    auto word_start = src.starts[i];
    auto word_end = word_start + src.lens[i];
    for (int j = word_start; j < word_end; j++) {
      data[pos++] = (float)src.chars[j];
    }
  }

  // Set the offsets
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.starts.size() + 1), 1));
  for (auto i = 1; i < src.starts.size() + 1; i++) {
    offsets[i] = last_chars[i];
  }
  offsets[src.starts.size()] = total_chars;

  // Set the validityBuffer
  size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * vcount, 1));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

bool nullable_float_vector::equals(const nullable_float_vector * const other) {
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

nullable_float_vector * nullable_float_vector::clone() {
  // Allocate
  auto * output = static_cast<nullable_float_vector *>(malloc(sizeof(nullable_float_vector)));

  // Copy the count
  output->count = count;

  // Copy the data
  output->data = static_cast<float *>(malloc(output->count*4));
  memcpy(output->data, data, output->count*4);

  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

nullable_float_vector * nullable_float_vector::from_words(const frovedis::words &src) const {
  // Allocate
  auto * output = static_cast<nullable_float_vector *>(malloc(sizeof(nullable_float_vector)));

  // Use placement new to construct nullable_float_vector from frovedis::words
  // in the pre-allocated memory
  return new (output) nullable_float_vector(src);
}

frovedis::words nullable_float_vector::to_words() const {
  frovedis::words output;
  if (count == 0) {
    return output;
  }

  // Set the lens
  output.lens.resize(count);
  for (auto i = 0; i < count; i++) {
    output.lens[i] = offsets[i + 1] - offsets[i];
  }

  // Set the starts
  output.starts.resize(count);
  for (int i = 0; i < count; i++) {
    output.starts[i] = offsets[i];
  }

  // Set the chars
  output.chars.resize(offsets[count]);
  //FIXME
//  frovedis::char_to_int(data, offsets[count], output.chars.data());

  return output;
}

nullable_float_vector * nullable_float_vector::filter(const std::vector<size_t> &matching_ids) const {
  // Get the frovedis::words representation of the input
  auto input_words = to_words();

  // Initialize the starts and lens
  std::vector<size_t> starts(matching_ids.size());
  std::vector<size_t> lens(matching_ids.size());

  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the start and len values
    starts[g] = input_words.starts[i];
    lens[g] = input_words.lens[i];
  }

  // Use starts, lens, and frovedis::concat_words to generate the frovedis::words
  // version of the filtered nullable_float_vector
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

  // Map the data from frovedis::words back to nullable_float_vector
  auto * output = nullable_float_vector::from_words(input_words);

  // Preserve the validityBuffer across the filter
  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the validity buffer
    output->set_validity(g, get_validity(i));
  }

  return output;
}

nullable_float_vector ** nullable_float_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_float_vector pointers
  auto ** output = static_cast<nullable_float_vector **>(malloc(sizeof(nullable_float_vector *) * bucket_counts.size()));

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

inline void nullable_double_vector::set_validity(const size_t idx,
                           const int32_t validity) {
    set_valid_bit(validityBuffer, idx, validity);
  }

inline uint32_t nullable_double_vector::get_validity(const size_t idx) const {
    return get_valid_bit(validityBuffer, idx);
  }

nullable_double_vector::nullable_double_vector(const frovedis::words &src) {
  // Set count
  count = src.lens.size();

  // Set dataSize
  auto total_chars = 0;
  for (size_t i = 0; i < src.lens.size(); i++) {
    total_chars += src.lens[i];
  }
  dataSize = total_chars;

  // Compute last_chars
  std::vector<size_t> last_chars(src.lens.size() + 1);
  auto sum = 0;
  for (auto i = 0; i < src.lens.size(); i++) {
    last_chars[i] = sum;
    sum += src.lens[i];
  }

  // Copy chars to data
  data = static_cast<double *>(malloc(sizeof(double) * total_chars));
  for (auto i = 0; i < count; i++) {
    auto pos = last_chars[i];
    auto word_start = src.starts[i];
    auto word_end = word_start + src.lens[i];
    for (int j = word_start; j < word_end; j++) {
      data[pos++] = (double)src.chars[j];
    }
  }

  // Set the offsets
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.starts.size() + 1), 1));
  for (auto i = 1; i < src.starts.size() + 1; i++) {
    offsets[i] = last_chars[i];
  }
  offsets[src.starts.size()] = total_chars;

  // Set the validityBuffer
  size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * vcount, 1));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

bool nullable_double_vector::equals(const nullable_double_vector * const other) {
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

nullable_double_vector * nullable_double_vector::clone() {
  // Allocate
  auto * output = static_cast<nullable_double_vector *>(malloc(sizeof(nullable_double_vector)));

  // Copy the count
  output->count = count;

  // Copy the data
  output->data = static_cast<double *>(malloc(output->count*8));
  memcpy(output->data, data, output->count*8);
  
  // Copy the validity buffer
  auto vbytes = frovedis::ceil_div(output->count, int32_t(64)) * sizeof(uint64_t);
  output->validityBuffer = static_cast<uint64_t *>(calloc(vbytes, 1));
  memcpy(output->validityBuffer, validityBuffer, vbytes);

  return output;
}

nullable_double_vector * nullable_double_vector::from_words(const frovedis::words &src) const {
  // Allocate
  auto * output = static_cast<nullable_double_vector *>(malloc(sizeof(nullable_double_vector)));

  // Use placement new to construct nullable_double_vector from frovedis::words
  // in the pre-allocated memory
  return new (output) nullable_double_vector(src);
}

frovedis::words nullable_double_vector::to_words() const {
  frovedis::words output;
  if (count == 0) {
    return output;
  }

  // Set the lens
  output.lens.resize(count);
  for (auto i = 0; i < count; i++) {
    output.lens[i] = offsets[i + 1] - offsets[i];
  }

  // Set the starts
  output.starts.resize(count);
  for (int i = 0; i < count; i++) {
    output.starts[i] = offsets[i];
  }

  // Set the chars
  output.chars.resize(offsets[count]);
  //FIXME
//  frovedis::char_to_int(data, offsets[count], output.chars.data());

  return output;
}

nullable_double_vector * nullable_double_vector::filter(const std::vector<size_t> &matching_ids) const {
  // Get the frovedis::words representation of the input
  auto input_words = to_words();

  // Initialize the starts and lens
  std::vector<size_t> starts(matching_ids.size());
  std::vector<size_t> lens(matching_ids.size());

  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the start and len values
    starts[g] = input_words.starts[i];
    lens[g] = input_words.lens[i];
  }

  // Use starts, lens, and frovedis::concat_words to generate the frovedis::words
  // version of the filtered nullable_double_vector
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

  // Map the data from frovedis::words back to nullable_double_vector
  auto * output = nullable_double_vector::from_words(input_words);

  // Preserve the validityBuffer across the filter
  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the validity buffer
    output->set_validity(g, get_validity(i));
  }

  return output;
}

nullable_double_vector ** nullable_double_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_double_vector pointers
  auto ** output = static_cast<nullable_double_vector **>(malloc(sizeof(nullable_double_vector *) * bucket_counts.size()));

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
