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

void nullable_varchar_vector::print() const {
  std::stringstream stream;

  stream << "nullable_varchar_vector @ " << this << " {\n";
  // Print count
  stream << "  COUNT: " << count << "\n";

  // Print dataSize
  stream << "  DATA SIZE: " << dataSize << "\n";

  // Print string values
  stream << "  VALUES: [ ";
  for (auto i = 0; i < count; i++) {
    if (check_valid(validityBuffer, i)) {
      stream << std::string(data,  offsets[i], offsets[i+1] - offsets[i]) << ", ";
    } else {
      stream << "#, ";
    }
  }

  // Print offsets
  stream << "]\n  OFFSETS: [";
  for (auto i = 0; i < count + 1; i++) {
      stream << offsets[i] << ", ";
  }

  // Print validityBuffer
  stream << "]\n  VALIDITY: [";
  for (auto i = 0; i < count; i++) {
      stream << check_valid(validityBuffer, i) << ", ";
  }

  // Print data
  stream << "]\n  DATA: [" << std::string(data, dataSize) << "]\n}\n";

  std::cout << stream.rdbuf() << std::endl;
}

bool nullable_varchar_vector::equals(const nullable_varchar_vector * const other) const {
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
  for (auto i = 0; i < count + 1; i++) {
    output = output && (offsets[i] == other->offsets[i]);
  }

  // Compare validityBuffer
  #pragma _NEC ivdep
  for (auto i = 0; i < count; i++) {
    output = output && (check_valid(validityBuffer, i) == check_valid(other->validityBuffer, i));
  }

  return output;
}

nullable_varchar_vector * nullable_varchar_vector::clone() const {
  // Allocate the output
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

nullable_varchar_vector * nullable_varchar_vector::filter(const std::vector<size_t> &matching_ids) const {
  // Allocate the output
  auto * output = static_cast<nullable_varchar_vector *>(malloc(sizeof(nullable_varchar_vector)));

  // Get the frovedis::words representation of the input
  auto input_words = varchar_vector_to_words(this);

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
  // version of the filtered nullable_varchar_vector
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
  words_to_varchar_vector(input_words, output);

  #pragma _NEC vector
  for (int g = 0; g < matching_ids.size(); g++) {
    // Fetch the original index
    int i = matching_ids[g];

    // Copy the validity buffer
    set_validity(output->validityBuffer, g, check_valid(validityBuffer, i));
  }

  return output;
}

nullable_varchar_vector ** nullable_varchar_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_varchar_vector pointers
  auto ** output = static_cast<nullable_varchar_vector **>(malloc(sizeof(nullptr) * bucket_counts.size()));

  // Allocate a bitmask array for multiple use
  std::vector<size_t> bitmask(bucket_assignments.size());

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
