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
#include "cyclone/cyclone.hpp"
#include "cyclone/transfer-definitions.hpp"
#include "cyclone/tuple_hash.hpp"
#include "frovedis/core/radix_sort.hpp"
#include "frovedis/core/set_operations.hpp"
#include "frovedis/core/utility.hpp"
#include "frovedis/dataframe/join.hpp"
#include <bitset>
#include <cmath>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

nullable_varchar_vector * project_eval(const nullable_varchar_vector *input_0)  {
  auto output_0_input_words = input_0->to_words();
  std::vector<size_t> output_0_starts(input_0->count);
  std::vector<size_t> output_0_lens(input_0->count);

  for ( int32_t i = 0; i < input_0->count; i++ ) {
    output_0_starts[i] = output_0_input_words.starts[i];
    output_0_lens[i] = output_0_input_words.lens[i];
  }

  std::vector<size_t> output_0_new_starts;
  std::vector<int> output_0_new_chars = frovedis::concat_words(
    output_0_input_words.chars,
    (const std::vector<size_t>&)(output_0_starts),
    (const std::vector<size_t>&)(output_0_lens),
    "",
    (std::vector<size_t>&)(output_0_new_starts)
  );

  output_0_input_words.chars = output_0_new_chars;
  output_0_input_words.starts = output_0_new_starts;
  output_0_input_words.lens = output_0_lens;

  auto *output_0 = new nullable_varchar_vector(output_0_input_words);

  for ( int i = 0; i < output_0->count; i++ ) {
    output_0->set_validity(i, input_0->get_validity(i));
  }

  return output_0;
}

void filter_test() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };
  std::vector<size_t> matching_ids { 1, 3, 5 };

  auto *input = new nullable_varchar_vector(data);
  input->set_validity(3, 0);

  std::cout << "================================================================================" << std::endl;
  std::cout << "FILTER TEST\n" << std::endl;

  std::cout << "Original nullable_varchar_vector:" << std::endl;
  input->print();

  const auto *output = input->filter(matching_ids);
  std::cout << "Filtered nullable_varchar_vector:" << std::endl;
  output->print();

  std::cout << "================================================================================" << std::endl;
  return;
}

void projection_test() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };

  auto *input = new nullable_varchar_vector(data);
  input->set_validity(3, 0);

  std::cout << "================================================================================" << std::endl;
  std::cout << "PROJECTION TEST\n" << std::endl;

  std::cout << "Original nullable_varchar_vector:" << std::endl;
  input->print();

  auto *output = project_eval(input);
  std::cout << "Cloned nullable_varchar_vector:" << std::endl;
  output->print();

  std::cout << "================================================================================" << std::endl;
  return;
}

void bucket_grouping_test_fail() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };
  std::vector<size_t> id_to_bucket { 0, 1, 0, 1, 0, 1, 0 };

  auto *input = new nullable_varchar_vector(data);
  input->set_validity(3, 0);

  auto output_0_input_words = input->to_words();

  std::vector<size_t> output_0_starts(id_to_bucket.size());
  std::vector<size_t> output_0_lens(id_to_bucket.size());

  for (auto i = 0; i < id_to_bucket.size(); i++ ) {
    if (id_to_bucket[i]) {
      output_0_starts[i] = output_0_input_words.starts[i];
      output_0_lens[i] = output_0_input_words.lens[i];
    }
  }

  std::vector<size_t> output_0_new_starts;
  std::vector<int> output_0_new_chars = frovedis::concat_words(
    output_0_input_words.chars,
    (const std::vector<size_t>&)(output_0_starts),
    (const std::vector<size_t>&)(output_0_lens),
    "",
    (std::vector<size_t>&)(output_0_new_starts)
  );

  output_0_input_words.chars = output_0_new_chars;
  output_0_input_words.starts = output_0_new_starts;
  output_0_input_words.lens = output_0_lens;

  auto *output_0 = new nullable_varchar_vector(output_0_input_words);

  auto o = 0;
  for (auto i = 0; i < id_to_bucket.size(); i++ ) {
    if (id_to_bucket[i]) {
      output_0->set_validity(o++, 1);
    }
  }

  std::cout << "================================================================================" << std::endl;
  std::cout << "BUCKET GROUPING TEST (FAILING)\n" << std::endl;

  input->print();

  std::cout << "ID_TO_BUCKET: [ ";
  for (auto i = 0; i < id_to_bucket.size(); i++) {
      std::cout << id_to_bucket[i] << ", ";
  }
  std::cout << "]\n\n";

  output_0->print();

  std::cout << "================================================================================" << std::endl;
  return;
}

void bucket_grouping_test_pass() {
  std::vector<std::string> data { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };
  std::vector<size_t> id_to_bucket { 0, 1, 0, 1, 0, 1, 0 };

  auto *input = new nullable_varchar_vector(data);
  input->set_validity(3, 0);

  auto output_0_input_words = input->to_words();

  std::vector<size_t> output_0_starts;
  std::vector<size_t> output_0_lens;
  output_0_starts.reserve(data.size());
  output_0_lens.reserve(data.size());

  for (auto i = 0; i < id_to_bucket.size(); i++ ) {
    if (id_to_bucket[i]) {
      output_0_starts.emplace_back(output_0_input_words.starts[i]);
      output_0_lens.emplace_back(output_0_input_words.lens[i]);
    }
  }

  std::vector<size_t> output_0_new_starts;
  std::vector<int> output_0_new_chars = frovedis::concat_words(
    output_0_input_words.chars,
    output_0_starts,
    output_0_lens,
    "",
    output_0_new_starts
  );

  output_0_input_words.chars = output_0_new_chars;
  output_0_input_words.starts = output_0_new_starts;
  output_0_input_words.lens = output_0_lens;

  auto *output_0 = new nullable_varchar_vector(output_0_input_words);

  auto o = 0;
  for (auto i = 0; i < id_to_bucket.size(); i++ ) {
    if (id_to_bucket[i]) {
      output_0->set_validity(o++, input->get_validity(i));
    }
  }

  std::cout << "================================================================================" << std::endl;
  std::cout << "BUCKET GROUPING TEST (PASSING)\n" << std::endl;

  input->print();

  std::cout << "ID_TO_BUCKET: [ ";
  for (auto i = 0; i < id_to_bucket.size(); i++) {
      std::cout << id_to_bucket[i] << ", ";
  }
  std::cout << "]\n\n";

  output_0->print();

  std::cout << "================================================================================" << std::endl;
  return;
}

void merge_test_pass() {
  // nullable_varchar_vector 1
  std::vector<std::string> data1 { "AIR", "MAIL", "RAIL", "SHIP", "TRUCK", "REG AIR", "FOB" };
  auto *input1 = new nullable_varchar_vector(data1);
  input1->set_validity(3, 0);

  // nullable_varchar_vector 2
  std::vector<std::string> data2 { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
  auto *input2 = new nullable_varchar_vector(data2);
  input2->set_validity(1, 0);
  input2->set_validity(4, 0);
  input2->set_validity(10, 0);

  auto batches = 2;
  nullable_varchar_vector **input_0_g = new nullable_varchar_vector* [batches];
  input_0_g[0] = input1;
  input_0_g[1] = input2;

  // Construct std::vector<frovedis::words>
  std::vector<frovedis::words> output_0_multi_words(2);
  for (int b = 0; b < batches; b++) {
    output_0_multi_words[b] = input_0_g[b]->to_words();
  }

  // Merge
  frovedis::words output_0_merged = frovedis::merge_multi_words(output_0_multi_words);
  auto *output_0 = new nullable_varchar_vector(output_0_merged);

  // Preserve validity buffers
  auto o = 0;
  for (int b = 0; b < batches; b++) {
    for (int i = 0; i < input_0_g[b]->count; i++) {
      output_0->set_validity(o++, input_0_g[b]->get_validity(i));
    }
  }

  std::cout << "================================================================================" << std::endl;
  std::cout << "MERGE MULTI WORDS (PASSING?)\n" << std::endl;

  input1->print();
  input2->print();
  output_0->print();

  std::cout << "================================================================================" << std::endl;
  return;
}

int main() {
  projection_test();
  filter_test();
  bucket_grouping_test_fail();
  bucket_grouping_test_pass();
  merge_test_pass();
}
