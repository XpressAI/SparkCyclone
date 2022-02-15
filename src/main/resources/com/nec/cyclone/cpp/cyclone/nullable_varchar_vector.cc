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
#include "frovedis/text/char_int_conv.hpp"
#include <stdlib.h>
#include <iostream>

nullable_varchar_vector * nullable_varchar_vector::from_words(const frovedis::words &src) {
  // Allocate
  auto * output = static_cast<nullable_varchar_vector *>(malloc(sizeof(nullable_varchar_vector)));

  // Use placement new to construct nullable_varchar_vector from frovedis::words
  // in the pre-allocated memory
  return new (output) nullable_varchar_vector(src);
}

nullable_varchar_vector::nullable_varchar_vector(const std::vector<std::string> &src) {
  // Initialize count
  count = src.size();

  // Initialize dataSize
  dataSize = 0;
  for (auto i = 0; i < src.size(); i++) {
    dataSize += src[i].size();
  }

  // Copy strings to data
  data = static_cast<char *>(malloc(sizeof(char) * dataSize));
  auto p = 0;
  for (auto i = 0; i < src.size(); i++) {
    for (auto j = 0; j < src[i].size(); j++) {
      data[p++] = src[i][j];
    }
  }

  // Set the offsets
  offsets = static_cast<int32_t *>(calloc(sizeof(int32_t) * (src.size() + 1), 1));
  offsets[0] = 0;
  for (auto i = 0; i < src.size(); i++) {
    offsets[i+1] = offsets[i] + src[i].size();
  }

  // Set the validityBuffer
  size_t vcount = frovedis::ceil_div(count, int32_t(64));
  validityBuffer = static_cast<uint64_t *>(calloc(sizeof(uint64_t) * vcount, 1));
  for (auto i = 0; i < vcount; i++) {
    validityBuffer[i] = 0xffffffffffffffff;
  }
}

nullable_varchar_vector::nullable_varchar_vector(const frovedis::words &src) {
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
  data = static_cast<char *>(malloc(sizeof(char) * total_chars));
  for (auto i = 0; i < count; i++) {
    auto pos = last_chars[i];
    auto word_start = src.starts[i];
    auto word_end = word_start + src.lens[i];
    for (int j = word_start; j < word_end; j++) {
      data[pos++] = (char)src.chars[j];
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

void nullable_varchar_vector::reset() {
  // Free the owned memory
  free(data);
  free(offsets);
  free(validityBuffer);

  // Reset the pointers and values
  data            = nullptr;
  offsets         = nullptr;
  validityBuffer  = nullptr;
  dataSize        = 0;
  count           = 0;
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
    output.lens[i] = offsets[i + 1] - offsets[i];
  }

  // Set the starts
  output.starts.resize(count);
  for (int i = 0; i < count; i++) {
    output.starts[i] = offsets[i];
  }

  // Set the chars
  output.chars.resize(offsets[count]);
  frovedis::char_to_int(data, offsets[count], output.chars.data());

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
           << "  DATA: [ ]\n";
  } else {
    // Print string values
    stream << "  VALUES: [ ";
    for (auto i = 0; i < count; i++) {
      if (get_validity(i)) {
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
        stream << get_validity(i) << ", ";
    }

    // Print data
    stream << "]\n  DATA: [" << std::string(data, dataSize) << "]\n";
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
  for (auto i = 0; i < count + 1; i++) {
    output = output && (offsets[i] == other->offsets[i]);
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
  auto * output = nullable_varchar_vector::from_words(input_words);

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

nullable_varchar_vector ** nullable_varchar_vector::bucket(const std::vector<size_t> &bucket_counts,
                                                           const std::vector<size_t> &bucket_assignments) const {
  // Allocate array of nullable_varchar_vector pointers
  auto ** output = static_cast<nullable_varchar_vector **>(malloc(sizeof(nullable_varchar_vector *) * bucket_counts.size()));

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
